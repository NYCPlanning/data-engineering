# EDDE Packager

Generates packaged outputs for EDDE data product.

## Output Dependency Graph

```mermaid
flowchart TD
    subgraph store[" edm.publishing "]
        old[Previous EDDE build]
    end

    subgraph build[" Build "]
        recipe[recipe]
        new[Current EDDE build]
    end

    subgraph postbuild[" DE EDDE Post-build "]
        step1["[1] site_conf_templates"]
        step2["[2] change_over_time"]
        step3["[3] resolved_pages_and_tables"]

        out1[["site_conf/<br/>demo.json, econ.json,<br/>hsaq.json, hopd.json,<br/>qlao.json"]]
        out2[["change_over_time/<br/>demographics/,<br/>economics/,<br/>housing_security/,<br/>quality_of_life/"]]
        out3[["resolved_pages_and_tables"]]
        out4[["{geography}_{geoid}_{category}.json files"]]
    end

    subgraph appeng[" App Eng. "]
        xlsx[District XLSX files]
    end

    recipe --> step1
    step1 --> out1

    old --> step2
    new --> step2
    step2 --> out2

    out1 --> step3
    out2 --> step3
    new --> step3
    step3 --> out3
    step3 --> out4

    out4 --> xlsx

    style store fill:#fff3e0,stroke:#f57c00
    style build fill:#f0f0f0,stroke:#999
    style postbuild fill:#e3f2fd,stroke:#1976d2
    style appeng fill:#f3e5f5,stroke:#7b1fa2
```

## Steps

1. **site_conf_templates** - Renders Jinja2 templates with recipe variables
2. **change_over_time** - Generates change datasets (12 files: 4 categories × 3 geographies)
3. **resolved_pages_and_tables** - Generates final JSON files for equity explorer frontend
