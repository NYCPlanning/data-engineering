Generate a graph of the codebase.

Run

``` sh
# from data-engineering root folder
cp experimental/tach_dep_graph/tach.toml ./
tach show --web

# Optionally, to refresh the graph if you make changes
tach sync
```

If we wanted to proceed with this type of thing, I'd worry about refactoring. This would break whenever files changed.
So we'd likely want to generate the tach.toml, which I think would be pretty trivial. (I reckon we could implement that in an hour) 

