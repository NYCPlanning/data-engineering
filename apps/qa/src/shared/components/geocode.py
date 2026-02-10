def geocode():
    import requests
    import streamlit as st

    st.title("Geosupport Flask API Demo")
    st.sidebar.markdown(
        """
    ### instructions:
    + `bl`: the __BBL__ (borough, block and lot) lookup ([read more](http://a030-goat.nyc.gov/goat/UserGuide#functionBL))
    + `bn`: the __BIN__ (building identification number) lookup ([read more](http://a030-goat.nyc.gov/goat/UserGuide#functionBN))
    + `1a`: given address finds the __tax lot__ associated with that address ([read more](http://a030-goat.nyc.gov/goat/UserGuide#function1B))
    + `1e`: given address finds the __street segment__ (more information about city services and political districts) ([read more](http://a030-goat.nyc.gov/goat/UserGuide#function1B))
    + `1b`: a combination of 1a and 1e ([read more](http://a030-goat.nyc.gov/goat/UserGuide#function1B))
    + `ap`: __address point__ lookup ([read more](http://a030-goat.nyc.gov/goat/UserGuide#function1B))

    ### Resources:
    For more information, refer to [Geographic Online Address Translator (GOAT)!](http://a030-goat.nyc.gov/goat/),
    or the official [Geosupport User Programing Guide](https://nycplanning.github.io/Geosupport-UPG/).
    [api-geosupport](https://github.com/NYCPlanning/api-geosupport)
    is developed using [Flask-RESTful](https://flask-restful.readthedocs.io/en/latest/)
    and [python-geosupport](https://python-geosupport.readthedocs.io/en/latest/) developed by Ian Shiland

    ### Try it now!
    [![Deploy to Heroku](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy/?template=https://github.com/NYCPlanning/api-geosupport)
    """
    )
    function = st.selectbox("pick a function", ["bl", "1b", "ap", "1a", "1e", "bn"])
    if function == "bl":
        BBL = st.text_input("BBL", "3044237501")
        url = f"https://geo.nycplanningdigital.com/geocode/bl?bbl={BBL}"
    elif function == "bn":
        BIN = st.text_input("BIN", "3324265")
        url = f"https://geo.nycplanningdigital.com/geocode/bn?bin={BIN}"
    else:
        boro = st.selectbox("borough", ["MN", "BK", "QN", "SI", "BX"], index=0)
        hnum = st.text_input("house number", "120")
        sname = st.text_input("street name", "Broadway")
        url = f"https://geo.nycplanningdigital.com/geocode/{function}?house_number={hnum}&street_name={sname}&borough={boro}"

    r = requests.get(url)
    st.write(url)
    st.json(r.text)
