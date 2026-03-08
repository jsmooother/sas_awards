"""
Region definitions mapping SAS API country names (Swedish) to user-facing regions.
"""

REGIONS = {
    "europe": {
        "label": "Europe",
        "icon": "🇪🇺",
        "countries": [
            "Belgien", "Bosnien Hercegovinien", "Bulgarien", "Cypern",
            "Danmark", "Estland", "Faroe Islands", "Finland", "Frankrike",
            "Grekland", "Irland", "Island", "Italien", "Kroatien",
            "Lettland", "Litauen", "Luxemburg", "Malta", "Montenegro",
            "Nederländerna", "Norge", "Polen", "Portugal", "Schweiz",
            "Spanien", "Storbritannien", "Sverige", "Tjeckien", "Tyskland",
            "Ungern", "Österrike",
        ],
    },
    "north_america": {
        "label": "USA & Canada",
        "icon": "🇺🇸",
        "countries": ["USA", "Kanada", "Grönland"],
    },
    "asia": {
        "label": "Asia",
        "icon": "🌏",
        "countries": ["Japan", "Korea", "Thailand", "Indien"],
    },
    "middle_east": {
        "label": "Middle East",
        "icon": "🕌",
        "countries": ["Förenade arabemiraten", "Israel", "Libanon", "Turkiet"],
    },
    "africa": {
        "label": "Africa",
        "icon": "🌍",
        "countries": ["Marocko"],
    },
}

_COUNTRY_TO_REGION = {}
for _key, _val in REGIONS.items():
    for _c in _val["countries"]:
        _COUNTRY_TO_REGION[_c] = _key


def country_to_region(country_name: str) -> str | None:
    return _COUNTRY_TO_REGION.get(country_name)


def region_countries(region_key: str) -> list[str]:
    r = REGIONS.get(region_key)
    return r["countries"] if r else []


def all_region_keys() -> list[str]:
    return list(REGIONS.keys())


def all_countries() -> list[str]:
    """Sorted list of all country names for filter dropdowns."""
    countries = set()
    for r in REGIONS.values():
        countries.update(r["countries"])
    return sorted(countries)
