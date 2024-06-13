# CDSE Downloader

Clients for searching and downloading data from Copernicus Data Space Ecosystem.

The structure of this client takes inspiration from a lot of clients I have used of the years. I designed some patterns around what I liked and found helpful and powerful in them.

- [sentinelsat](https://github.com/sentinelsat/sentinelsat): downloading
- [NASA python-cmr](https://github.com/nasa/python_cmr): auth, searching products
- [pystac-client](https://github.com/stac-utils/pystac-client): datetime and aoi parsing
- [Google Earth Engine](https://github.com/google/earthengine-api): filters

## TODO

- [x] Auth
    - [x] create tokens
    - [x] refresh tokens
- [ ] OData
    - [x] query products
    - [X] query deleted products 
    - [ ] query by list
    - [ ] query nodes
- [x] OpenSearch
    - [x] Query products
- [x] Download products
    - [x] download single product
    - [x] download multiple products in parallel
    - [x] download by id or name
- [ ] Subscriptions?
- [ ] CLI?


## Usage

### OData

#### Product Search

To search OData use `ProductSearch` to query the API. Specify a collection, sensing date, publication date, area, and can further filter using lists of `AttributeFilter`
```python
from cdse_dl.odata.filter import AttributeFilter
from cdse_dl.odata.search import ProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C")
]
area = "POINT (12.4577739 41.9077492)"

search = ProductSearch(
    collection="SENTINEL-2",
    area=area,
    date="2020-01-01/2020-01-02",
    filters=filters,
)
search.get(10)
```

You `filters` to build complex query patterns using OData's ability to filter on Attributes of the products. Use `or_`, `and_` or `not_` to combine or invert filters.

Any filters passed in the the list are `and`-ed together to build the final string.

Filter Methods:
- Greater then: `gt`
- Less then: `lt`
- Greater then or equal : `gte`
- Less then or equal: `lte`
- Equal to: `eq`
- Not equal to: `neq`
- String contains: `contains`
- String starts with: `startswith`
- String ends with: `endswith`

```python
from cdse_dl.odata.filter import AttributeFilter, Filter
from cdse_dl.odata.search import ProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C"),
    Filter.or_([
        AttributeFilter.eq("tileId","32TPN"),
        AttributeFilter.eq("tileId","33TUH"),
    ]),
    Filter.and_([
        AttributeFilter.gt("cloudCover", 10),
        AttributeFilter.lt("cloudCover", 50),
    ]),
    AttributeFilter.eq("processorVersion","05.00").not_()
]
area = "POINT (12.4577739 41.9077492)"

search = ProductSearch(
    collection="SENTINEL-2",
    date="2020-01-01/2020-02-01",
    filters=filters,
    expand="Attributes"
)
print(search.hits())
products = search.get(20)
```

You can use other params such as `order_by`, `expand`, `skip`, and `top` to modify your search. `skip` and `top` are used during `.get()` and `.get_all()` and unless necessary can be ignored.

`expand` will add full metadata of each returned result. You can expand Attributes or Assets.

```python
from cdse_dl.odata.filter import AttributeFilter
from cdse_dl.odata.search import ProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C")
]
area = "POINT (12.4577739 41.9077492)"

search = ProductSearch(
    collection="SENTINEL-2",
    area=area,
    date="2020-01-01/2020-01-02",
    filters=filters,
    order_by="ContentDate/Start",
    expand="Attributes"
)
search.get(1)
```

#### Deleted Product Search

To search for a specific deleted product, you can use OData's deleted product API with `DeletedProductSearch`.
```python
from cdse_dl.odata.search import DeletedProductSearch

search = DeletedProductSearch(
    collection="SENTINEL-2",
    name="S2A_MSIL1C_20210331T100021_N0500_R122_T32TQM_20230218T121926.SAFE",
)
search.get(1)
```

To find products deleted during a specified date range, use the `deletion_date` filter
```python
from cdse_dl.odata.search import DeletedProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C")
]

search = DeletedProductSearch(
    collection="SENTINEL-2",
    deletion_date="2024-01-31/2024-02-01",
    filters=filters,
)
search.hits()
```

To find products from published in a specified date range that have been deleted, use the `origin_date` filter
```python
from cdse_dl.odata.search import DeletedProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C")
]

search = DeletedProductSearch(
    collection="SENTINEL-2",
    origin_date="2022-02-01/2022-02-10",
    filters=filters,
)
search.hits()
```
### OpenSearch

#### Product Search

### OData

#### Product Search

```python
from cdse_dl.opensearch.search import ProductSearch

search = ProductSearch(
    collection="Sentinel2",
    point=(12.4577739,41.9077492),
    product_type="S2MSI1C",
    date="2000-01-01/2024-05-01",
)
items = list(search.get(10))
```

### Download

To download a product, use the Downloader to manage downloading.
```python
from cdse_dl.odata.search import ProductSearch
from cdse_dl.download import Downloader

name = "S2A_MSIL1C_20200116T100341_N0208_R122_T33TUH_20200116T103621.SAFE"

product = ProductSearch(name=name).get(1)[0]

downloader = Downloader()
downloader.download(product, "tmp")
```

You can auth from environment variables, netrc, or pass your own personal credentials.
```python
from cdse_dl.download import Downloader
from cdse_dl.auth import Credentials

creds = Credentials.from_login("username", "password")
downloader = Downloader(credentials=creds)
```

To download multiple products, use `download_all`. The download manager will manage the 4 concurrent product limit of downloads on the session.
```python
from cdse_dl.download import Downloader
from cdse_dl.odata.filter import AttributeFilter
from cdse_dl.odata.search import ProductSearch

filters = [
    AttributeFilter.eq("productType","S2MSI1C")
]

products = ProductSearch(collection="SENTINEL-2",filters=filters).get(5)

downloader = Downloader()
downloader.download_all(products, "tmp")
```

If you want to interact with files over the s3 api, you can do so using the s3fs session from `get_s3fs_session`, which authorizes you to the CDSE s3 api.

This endpoint may be higher performance from my testing.

```python
from cdse_dl.auth import get_s3fs_session
from fsspec.callbacks import TqdmCallback

fs = get_s3fs_session()
tqdm_kwargs = {"unit":"files"}

remote_path = "eodata/Sentinel-2/MSI/L1C/2021/07/11/S2B_MSIL1C_20210711T095029_N0301_R079_T34UEC_20210711T110140.SAFE"
local_path = "S2B_MSIL1C_20210711T095029_N0301_R079_T34UEC_20210711T110140.SAFE"

_ = fs.get(
    remote_path,
    local_path,
    recursive=True,
    callback=TqdmCallback(tqdm_kwargs=tqdm_kwargs),
)
```