# docker-geosupport
![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/NYCPlanning/docker-geosupport) ![GitHub Workflow Status](https://img.shields.io/github/workflow/status/NYCPlanning/docker-geosupport/Create%20geosupport%20docker%20image)

## About: 
This is a repository for dockerized [geosupport](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page) linux desktop version. 
Thanks to [python-geosupport](https://github.com/ishiland/python-geosupport) python binding package for geosupport, we are able to localize our geocoding process. 

## Instructions: 
> Note that as of 2020/10/13, the nycplanning/docker-geosupport image will be automatically updated whenever there's a new major rlease or upad release on [Bytes of the Big Apple](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page). Manual updates are still available but discouraged, and will be gradually deprecated.

### Build on your own machine
1. Make sure you have docker installed
2. 
    ```
    RELEASE=20a
    MAJOR=20
    MINOR=1
    PATCH=2
    docker build --file Dockerfile \
            --build-arg RELEASE=$RELEASE \
            --build-arg MAJOR=$MAJOR \
            --build-arg MINOR=$MINOR \
            --build-arg PATCH=$PATCH \
            --tag nycplanning/docker-geosupport:$MAJOR.$MINOR.$PATCH .
    ````
    
3. make sure the submodule for `python-geosupport` is updated and available on your local

Pulling repo for the first time and pull files from submodule

```
git submodule update --init --recursive
```

Updating submodule

```
git submodule update --remote
```

### Build through Issue (DEPRECATING ...)
1. Click on the issue icon and make a new issue.
2. follow the "build" issue template to open an issue.
3. make sure you specify the versioning info in the issue body.
4. When github actions finishes building the docker image and pushes the docker image to docker hub, it will automatically comment and close the issue that triggered the task. 

### Build through CI (DEPRECATING ...)
1. make sure you include version specifications in commit message
    ```
    git commit -m 'RELEASE=20a MAJOR=20 MINOR=1 PATCH=0'
    ```
## Note: 
1. if there's no UPAD available, set PATCH=0. 
2. You can find Geosupport desktop edition and UPAD related information on [Bytes of the Big Apple](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page)
