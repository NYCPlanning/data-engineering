import importlib
import os
import re
import subprocess
import sys
from pathlib import Path

import typer

from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.config import BUILD_STAGE_KEY
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils.logging import logger

app = typer.Typer(add_completion=False)


def upload_build(build_path: Path, recipe_lock_path: Path | None = None) -> dict:
    """Upload a build to the destination configured in the recipe."""
    recipe = plan.recipe_from_yaml(
        recipe_lock_path or (Path(build_path).parent / "recipe.lock.yml")
    )

    stage_config = recipe.stage_config[BUILD_STAGE_KEY]
    # TODO: eventually we should add stage_config defaults in lifecycle.config
    assert stage_config.destination, "A destination must be defined"

    connector_key = stage_config.destination_key or recipe.product

    # For builds, use recipe.build_name as the version/build identifier
    # (recipe.version is the dataset version like "24v1", not the build name)
    if not recipe.build_name:
        raise ValueError(
            "recipe.build_name must be set to upload a build. "
            "This should be set during planning from the BUILD_NAME environment variable."
        )

    result = connectors[stage_config.destination].push(
        version=recipe.build_name,
        build_path=build_path,
        key=connector_key,
        connector_args=stage_config.get_connector_args_dict(),
        # TODO: eventually also pass the metadata from the build stage output, which would allow us to skip passing the build path
    )
    return result


def run_single_command(
    product_path: Path,
    recipe_lock_path: Path,
    command_name: str,
    build_directory: Path | None = None,
) -> None:
    """Execute a single build command from the recipe.

    Args:
        product_path: Path to the product directory
        recipe_lock_path: Path to recipe.lock.yml
        command_name: Name of the command to execute. Special names:
            - "load_recipe_data": Load source data from recipe
            - "export_recipe_data": Export datasets and write metadata
        build_directory: Optional build directory path (for loading recipe from build dir)
    """
    # Load recipe
    recipe = plan.recipe_from_yaml(recipe_lock_path)

    # Set environment variables hierarchically:
    # 1. Recipe-level env vars (from recipe.env)
    # 2. Stage-level env vars (from stage_config.build.env)
    # 3. Command-specific env vars (set below if executing a command)

    # 1. Recipe-level env vars
    if recipe.env:
        for key, value in recipe.env.items():
            os.environ[key] = value
            logger.info(f"Set recipe env: {key}={value}")

    # Parse BUILD_ENGINE_SERVER to extract components for dbt profiles (if set)
    if "BUILD_ENGINE_SERVER" in os.environ:
        server_match = re.match(
            r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)",
            os.environ["BUILD_ENGINE_SERVER"],
        )
        if server_match:
            os.environ["BUILD_ENGINE_USER"] = server_match.group(1)
            os.environ["BUILD_ENGINE_PASSWORD"] = server_match.group(2)
            os.environ["BUILD_ENGINE_HOST"] = server_match.group(3)
            os.environ["BUILD_ENGINE_PORT"] = server_match.group(4)

    # Construct BUILD_ENGINE connection string (if BUILD_ENGINE_DB and BUILD_ENGINE_SCHEMA are set)
    if (
        os.environ.get("BUILD_ENGINE_SERVER")
        and os.environ.get("BUILD_ENGINE_DB")
        and os.environ.get("BUILD_ENGINE_SCHEMA")
    ):
        build_engine = (
            f"{os.environ['BUILD_ENGINE_SERVER']}/"
            f"{os.environ['BUILD_ENGINE_DB']}?"
            f"options=--search_path%3D{os.environ['BUILD_ENGINE_SCHEMA']},public"
        )
        os.environ["BUILD_ENGINE"] = build_engine
        logger.info(f"Set BUILD_ENGINE: {build_engine}")

    # Handle special command names
    if command_name == "load_recipe_data":
        logger.info("Executing load_recipe_data step")
        from dcpy.lifecycle.builds import load as build_load

        # Determine build directory for setting BUILD_ENV_OUTPUT_DIR
        if build_directory:
            os.environ["BUILD_ENV_OUTPUT_DIR"] = str(build_directory)
            logger.info(f"Set BUILD_ENV_OUTPUT_DIR: {build_directory}")

        # Configure file logging
        if build_directory:
            logger.set_file_output(build_directory)

        # Load source data from recipe into build database
        load_result = build_load.load_source_data_from_resolved_recipe(
            recipe_or_path=recipe_lock_path,
            clear_pg_schema=True,
        )
        logger.info(f"Loaded {len(load_result.datasets)} datasets")
        return

    elif command_name == "export_recipe_data":
        logger.info("Executing export_recipe_data step")
        from dcpy.lifecycle.builds import export as build_export
        from dcpy.lifecycle.builds import metadata as build_metadata
        from dcpy.lifecycle.config import get_build_dir

        # Determine build directory
        if build_directory:
            os.environ["BUILD_ENV_OUTPUT_DIR"] = str(build_directory)
            logger.info(f"Set BUILD_ENV_OUTPUT_DIR: {build_directory}")

        # If recipe has exports, run the export step
        if recipe.exports:
            logger.info("Running export step...")
            output_path = build_export.export(
                recipe_lock_path=recipe_lock_path,
            )
            if not output_path:
                raise RuntimeError("Export failed to return output path")
        else:
            # No exports defined - use the lifecycle build directory
            product_name = product_path.name
            assert recipe.build_name, (
                "Recipe must have a build_name to determine build directory"
            )
            output_path = get_build_dir(product_name, recipe.build_name)
            logger.info(f"Using build directory as output path: {output_path}")
            if not output_path.exists():
                output_path.mkdir(parents=True, exist_ok=True)

        # Write build metadata
        build_metadata.write_build_metadata(
            recipe=recipe,
            output_folder=output_path,
            load_result=None,  # We don't have load_result in this context
        )
        logger.info(f"Export and metadata write completed. Output: {output_path}")
        return

    # Not a special command - execute a build command from recipe
    if BUILD_STAGE_KEY not in recipe.stage_config:
        raise ValueError(
            f"Recipe does not contain '{BUILD_STAGE_KEY}' stage configuration"
        )

    build_config = recipe.stage_config[BUILD_STAGE_KEY]

    # 2. Stage-level env vars
    if build_config.env:
        for key, value in build_config.env.items():
            os.environ[key] = value
            logger.info(f"Set stage env: {key}={value}")

    # Find the command by name
    cmd = None
    for c in build_config.commands:
        if c.name == command_name:
            cmd = c
            break

    if cmd is None:
        raise ValueError(
            f"Command '{command_name}' not found in recipe. "
            f"Available commands: {[c.name for c in build_config.commands]}"
        )

    # Execute the command
    logger.info(f"Executing build command '{cmd.name}': {cmd.run}")

    if cmd.command_type == "shell":
        # Prepare environment variables (merge command env with current env)
        env = os.environ.copy()
        if cmd.env:
            env.update(cmd.env)
            logger.info(f"  Setting env vars: {', '.join(cmd.env.keys())}")

        # Execute as shell command in product directory
        result = subprocess.run(
            cmd.run,
            shell=True,
            cwd=product_path,
            capture_output=True,
            text=True,
            env=env,
        )

        if result.returncode != 0:
            logger.error(f"Command '{cmd.name}' failed:")
            logger.error(f"  stdout: {result.stdout}")
            logger.error(f"  stderr: {result.stderr}")

            error_msg = f"Build command '{cmd.name}' failed with exit code {result.returncode}\n"
            if result.stdout:
                error_msg += f"\nSTDOUT:\n{result.stdout}\n"
            if result.stderr:
                error_msg += f"\nSTDERR:\n{result.stderr}\n"

            raise RuntimeError(error_msg)

        if result.stdout:
            logger.info(f"  stdout: {result.stdout}")
        logger.info("=" * 80)
        logger.info(f"✓ Command '{cmd.name}' completed successfully")
        logger.info("=" * 80)

    elif cmd.command_type == "python":
        # Execute as Python module using importlib
        logger.info(f"Executing Python module: {cmd.run}")

        # Add product path to sys.path
        if str(product_path) not in sys.path:
            sys.path.insert(0, str(product_path))
            logger.info(f"  Added {product_path} to sys.path")

        try:
            # Parse module and function
            if ":" in cmd.run:
                module_path, function_name = cmd.run.split(":", 1)
            else:
                module_path = cmd.run
                function_name = None

            logger.info(f"  Attempting to import module: {module_path}")

            # Import the module
            module = importlib.import_module(module_path)

            logger.info(f"  Successfully imported: {module}")

            # Set environment variables from command
            if cmd.env:
                for key, value in cmd.env.items():
                    os.environ[key] = value
                logger.info(f"  Setting env vars: {', '.join(cmd.env.keys())}")

            # Execute the function or module
            if function_name:
                func = getattr(module, function_name)
                func()
            else:
                if hasattr(module, "main"):
                    module.main()

            logger.info("=" * 80)
            logger.info(f"✓ Command '{cmd.name}' completed successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Command '{cmd.name}' failed:")
            logger.error(f"  Error: {str(e)}")

            error_msg = f"Build command '{cmd.name}' failed\n"
            error_msg += f"\nError importing/executing Python module:\n{str(e)}\n"

            raise RuntimeError(error_msg) from e


def build(
    product_path: Path,
    recipe_lock_or_path: "plan.Recipe | Path | None" = None,
    build_directory: Path | None = None,
) -> Path:
    """Execute build commands from recipe and return output path.

    Args:
        product_path: Path to the product directory
        recipe_lock_or_path: Recipe model, Path to recipe.lock.yml, or None
        build_directory: Optional build directory path. If provided and recipe_lock_or_path is None,
                        will look for recipe.lock.yml in this directory.

    Returns:
        Path to the build output directory
    """
    # Accept either a Recipe model or a Path to recipe.lock.yml
    recipe_lock_path: Path | None
    if recipe_lock_or_path is None:
        # Load from build directory or product directory
        if build_directory:
            recipe_lock_path = build_directory / "recipe.lock.yml"
            logger.info(f"Loading recipe from build directory: {recipe_lock_path}")
        else:
            # Backward compatibility: fall back to product directory
            recipe_lock_path = product_path / "recipe.lock.yml"
            logger.info(f"Loading recipe from product directory: {recipe_lock_path}")

        recipe = plan.recipe_from_yaml(recipe_lock_path)
    elif isinstance(recipe_lock_or_path, Path):
        # Path provided: load from path
        recipe = plan.recipe_from_yaml(recipe_lock_or_path)
        recipe_lock_path = recipe_lock_or_path
    else:
        # Recipe model provided directly
        recipe = recipe_lock_or_path
        recipe_lock_path = None

    # Get build stage config
    if BUILD_STAGE_KEY not in recipe.stage_config:
        raise ValueError(
            f"Recipe does not contain '{BUILD_STAGE_KEY}' stage configuration. "
            f"Add stage_config.{BUILD_STAGE_KEY}.commands to recipe.yml"
        )

    build_config = recipe.stage_config[BUILD_STAGE_KEY]

    if not build_config.commands:
        raise ValueError(
            "No build commands specified in recipe stage_config.build.commands"
        )

    # Set environment variables hierarchically:
    # 1. Recipe-level env vars (from recipe.env)
    # 2. Stage-level env vars (from stage_config.build.env)
    # 3. Command-specific env vars (set per command below)

    # 1. Recipe-level env vars
    if recipe.env:
        for key, value in recipe.env.items():
            os.environ[key] = value
            logger.info(f"Set recipe env: {key}={value}")

    # 2. Stage-level env vars
    if build_config.env:
        for key, value in build_config.env.items():
            os.environ[key] = value
            logger.info(f"Set stage env: {key}={value}")

    # Parse BUILD_ENGINE_SERVER to extract components for dbt profiles (if set)
    # Format: postgresql://user:password@host:port
    if "BUILD_ENGINE_SERVER" in os.environ:
        server_match = re.match(
            r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)",
            os.environ["BUILD_ENGINE_SERVER"],
        )
        if server_match:
            os.environ["BUILD_ENGINE_USER"] = server_match.group(1)
            os.environ["BUILD_ENGINE_PASSWORD"] = server_match.group(2)
            os.environ["BUILD_ENGINE_HOST"] = server_match.group(3)
            os.environ["BUILD_ENGINE_PORT"] = server_match.group(4)

    # Construct BUILD_ENGINE connection string (if BUILD_ENGINE_DB and BUILD_ENGINE_SCHEMA are set)
    build_engine = None
    if (
        os.environ.get("BUILD_ENGINE_SERVER")
        and os.environ.get("BUILD_ENGINE_DB")
        and os.environ.get("BUILD_ENGINE_SCHEMA")
    ):
        build_engine = (
            f"{os.environ['BUILD_ENGINE_SERVER']}/"
            f"{os.environ['BUILD_ENGINE_DB']}?"
            f"options=--search_path%3D{os.environ['BUILD_ENGINE_SCHEMA']},public"
        )
        os.environ["BUILD_ENGINE"] = build_engine
        logger.info(f"Set BUILD_ENGINE: {build_engine}")
    # Execute each build command
    for cmd in build_config.commands:
        logger.info(f"Executing build command '{cmd.name}': {cmd.run}")

        if cmd.command_type == "shell":
            # Prepare environment variables (merge command env with current env)
            env = os.environ.copy()
            if cmd.env:
                env.update(cmd.env)
                logger.info(f"  Setting env vars: {', '.join(cmd.env.keys())}")

            # Execute as shell command in product directory
            result = subprocess.run(
                cmd.run,
                shell=True,
                cwd=product_path,
                capture_output=True,
                text=True,
                env=env,
            )

            if result.returncode != 0:
                logger.error(f"Command '{cmd.name}' failed:")
                logger.error(f"  stdout: {result.stdout}")
                logger.error(f"  stderr: {result.stderr}")

                # Include stdout/stderr in exception message for better error visibility
                error_msg = f"Build command '{cmd.name}' failed with exit code {result.returncode}\n"
                if result.stdout:
                    error_msg += f"\nSTDOUT:\n{result.stdout}\n"
                if result.stderr:
                    error_msg += f"\nSTDERR:\n{result.stderr}\n"

                raise RuntimeError(error_msg)

            if result.stdout:
                logger.info(f"  stdout: {result.stdout}")
            logger.info("=" * 80)
            logger.info(f"✓ Command '{cmd.name}' completed successfully")
            logger.info("=" * 80)

        elif cmd.command_type == "python":
            # Execute as Python module using importlib
            # Expected format for cmd.run: "module.path:function_name" or just "module.path"
            # If no function specified, runs module.__main__
            logger.info(f"Executing Python module: {cmd.run}")

            # Add product path to sys.path so modules can be imported
            if str(product_path) not in sys.path:
                sys.path.insert(0, str(product_path))
                logger.info(f"  Added {product_path} to sys.path")
            else:
                logger.info(f"  {product_path} already in sys.path")

            logger.info(f"  sys.path[0]: {sys.path[0]}")
            logger.info(f"  Current working directory: {os.getcwd()}")

            try:
                # Parse module and function
                if ":" in cmd.run:
                    module_path, function_name = cmd.run.split(":", 1)
                else:
                    module_path = cmd.run
                    function_name = None

                logger.info(f"  Attempting to import module: {module_path}")

                # Import the module
                module = importlib.import_module(module_path)

                logger.info(f"  Successfully imported: {module}")

                # Set environment variables from command
                if cmd.env:
                    for key, value in cmd.env.items():
                        os.environ[key] = value
                    logger.info(f"  Setting env vars: {', '.join(cmd.env.keys())}")

                # Execute the function or module
                if function_name:
                    # Call the specified function
                    func = getattr(module, function_name)
                    func()
                else:
                    # Run the module's __main__ code
                    # Module has already been imported, which runs top-level code
                    # If it has a main() function, call it
                    if hasattr(module, "main"):
                        module.main()

                logger.info("=" * 80)
                logger.info(f"✓ Command '{cmd.name}' completed successfully")
                logger.info("=" * 80)

            except Exception as e:
                logger.error(f"Command '{cmd.name}' failed:")
                logger.error(f"  Error: {str(e)}")

                error_msg = f"Build command '{cmd.name}' failed\n"
                error_msg += f"\nError importing/executing Python module:\n{str(e)}\n"

                raise RuntimeError(error_msg) from e
            finally:
                # Keep product_path in sys.path for the duration of the build
                # Removing it can cause issues with cached module imports
                pass

    # Export datasets from PostgreSQL and write build_metadata.json
    from dcpy.lifecycle.builds import export as build_export
    from dcpy.lifecycle.builds import metadata as build_metadata
    from dcpy.lifecycle.config import get_build_dir

    # If recipe has exports, run the export step
    if recipe.exports:
        logger.info("Running export step...")
        output_path = build_export.export(
            recipe_lock_path=recipe_lock_path or (product_path / "recipe.lock.yml"),
        )
        if not output_path:
            raise RuntimeError("Export failed to return output path")
    else:
        # No exports defined - use the lifecycle build directory
        # Products may write outputs directly to this directory
        product_name = product_path.name
        assert recipe.build_name, (
            "Recipe must have a build_name to determine build directory"
        )
        output_path = get_build_dir(product_name, recipe.build_name)
        logger.info(f"Using build directory as output path: {output_path}")
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)

    # Write build metadata
    build_metadata.write_build_metadata(
        recipe=recipe,
        output_folder=output_path,
        load_result=None,  # We don't have load_result in build context
    )

    logger.info(f"Build completed successfully. Output: {output_path}")
    return output_path


@app.command("upload")
def _upload_build(
    build_path: Path,
    recipe_lock_path: Path = typer.Option(
        None,
        "--recipe-path",
        "-r",
        help="Path of recipe lock file to use",
    ),
):
    """Upload a build to the destination configured in the recipe."""
    result = upload_build(build_path, recipe_lock_path)
    typer.echo(result)
