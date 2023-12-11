import requests
import streamlit as st

BLOG_URL = "https://api.rss2json.com/v1/api.json?rss_url=https://medium.com/feed/nyc-planning-digital/tagged/data-engineering"
CSS = """
        .blog-card {
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
        transition: 0.3s;
        width: 100%;
        height: 100%;
        margin-bottom: 30px;
        }

        .blog-card:hover {
        box-shadow: 0 8px 16px 0 rgba(0,0,0,0.2);
        }

        .blog-card img {
            max-height: 200px;
            overflow: hidden;
            object-fit: cover;
        }

        .container {
        padding: 2px 16px;
        }
    """


@st.cache_data(ttl=30000)
def retrieve_blog_posts():
    print(requests.get(BLOG_URL, timeout=10).json())
    return requests.get(BLOG_URL, timeout=10).json()["items"]


def display_blog():
    for post in retrieve_blog_posts():
        st.markdown(
            f"""
        <style>
        {CSS}
        </style>
        <div class="blog-card">
        <img src="{post['thumbnail']}" alt="Avatar" style="width:100%;">
            <div class="container">
                <a href="{post['link']}"><h4><b>{post['title']}</b></h4></a>
                <p>By {post['author']}</p>
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )


def home():
    st.sidebar.success("Select a dataset above.")
    st.markdown(
        """
    <h1 style="font-size:3rem;">📊 Data Engineering QAQC</h1>
    """,
        unsafe_allow_html=True,
    )
    st.info(
        """
    Welcome to the QAQC Portal 🎉! This is a one stop shop for
    core **Data Engineering** data products, including PLUTO, Zoning Tax Lots,
    The Facilities database and more.

    You can find data QAQC dashboard through the dropdown menu on the left.
    If you have questions, let us know on [gitter](https://gitter.im/NYCPlanning/community)
    or open up an [issue](https://github.com/NYCPlanning/data-engineering/issues) on Github.
    """
    )

    st.header("Read more on Medium")
    display_blog()
