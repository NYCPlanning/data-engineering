# db-factfinder
[![PyPI version](https://badge.fury.io/py/pff-factfinder.svg)](https://badge.fury.io/py/pff-factfinder)

data ETL for population fact finder (decennial + acs)

# Instructions
1. initialize
```python
from factfinder.calculate import Calculate

calculate = Calculate(
    api_key='XXXXXXXXXXXXXXX',
    year=2019,
    source="acs",
    geography='2010_to_2020'
)
```
> or for decennial calculations:
```python

calculate = Calculate(
    api_key='XXXXXXXXXXXXXXX',
    year=2010,
    source="decennial",
    geography='2010_to_2020'
)
```
2. Calculate
> Calling `calculate`
>    1. Downloads necessary census variables,
>    2. Aggregates using appropriate technique (both vertically and horizontally)
>    3. Calculates c, e, m, p, z fields
>    4. Rounds calculated values based on standards in the metadata
>    5. Cleans edge-cases of 0 and null values
```python
df = calculate('pop', 'tract')
df = calculate('pop', 'borough')
df = calculate('pop', 'city')
df = calculate('pop', 'NTA')
df = calculate('mdage', 'CDTA')
```
