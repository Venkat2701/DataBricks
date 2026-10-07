"""
Per-site settings for the silver scraper, keyed by the host of the company URL
in the bronze table (without "www."). Companies missing here use DEFAULT_RULE,
which scrapes the whole site from the bronze URL.

base_url          Only pages under this address are listed and scraped (the US
                  English part of the site).
start_url         First page to scrape. Defaults to base_url.
sitemaps          Sitemap files to read. Defaults to the ones in robots.txt.
exclude           Regex on the lowercase path below base_url; matching pages are
                  ignored completely.
product_pattern   Regex on the lowercase path below base_url that marks product
                  pages. Without it, product pages are found by keywords.
category_segment  Which path segment below base_url (0 = first) names the
                  product category on product pages.
"""

DEFAULT_RULE = {
    "base_url": None,
    "start_url": None,
    "sitemaps": None,
    "exclude": None,
    "product_pattern": None,
    "category_segment": 0,
}

SITE_RULES = {
    "deere.com": {
        "base_url": "https://www.deere.com/en-us/",
        "product_pattern": r"^products-(and-)?solutions(/|$)",
        "category_segment": 1,
    },
    "cat.com": {
        "base_url": "https://www.cat.com/",
    },
    "casece.com": {
        "base_url": "https://www.casece.com/en-us/northamerica/",
        "product_pattern": r"^products(/|$)",
        "category_segment": 1,
    },
    "bobcat.com": {
        "base_url": "https://www.bobcat.com/na/en/",
        "product_pattern": r"^(equipment|attachments)(/|$)",
        "category_segment": 1,
    },
    "komatsu.com": {
        "base_url": "https://www.komatsu.com/en-us/",
        # ~180,000 spare-part pages; not useful for the dashboard.
        "exclude": r"^products/parts(/|$)",
        "product_pattern": r"^products/equipment(/|$)",
        "category_segment": 2,
    },
    "kubotausa.com": {
        "base_url": "https://www.kubotausa.com/",
        "product_pattern": r"^equipment-(series|category)(/|$)",
        "category_segment": 1,
    },
    "jcb.com": {
        "base_url": "https://www.jcb.com/en-US/",
        "product_pattern": r"^products/(machines|attachments)(/|$)",
        "category_segment": 2,
    },
    "agriculture.newholland.com": {
        "base_url": "https://agriculture.newholland.com/en-us/nar/",
        "product_pattern": r"^products(/|$)",
        "category_segment": 1,
    },
    "agcocorp.com": {
        "base_url": "https://www.agcocorp.com/us/en/",
        "start_url": "https://www.agcocorp.com/us/en/home.html",
        "product_pattern": r"^home/brands-and-solutions(/|$)",
        "category_segment": 2,
    },
    "terex.com": {
        # No sitemap; pages are found by following links from the homepage.
        # English pages are at the root; other languages are under /de/, /fr/, /pt-br/, ...
        "base_url": "https://www.terex.com/",
        "exclude": r"^[a-z]{2}(-[a-z]{2})?(/|$)",
    },
    "volvoce.com": {
        # The site root is a country chooser. The US section moved from
        # /united-states/en-us/ to /en-us/ in October 2026.
        "base_url": "https://www.volvoce.com/en-us/",
        "product_pattern": r"^(products|attachments)(/|$)",
        "category_segment": 1,
    },
    "hitachicm.us": {
        "base_url": "https://www.hitachicm.com/us/en/",
        "product_pattern": r"^products(/|$)",
        "category_segment": 1,
    },
    "wackerneuson.com": {
        # The site root is a country chooser; robots.txt lists ~40 country sitemaps.
        "base_url": "https://www.wackerneuson.com/us/",
        "sitemaps": ["https://www.wackerneuson.com/us/sitemap.xml"],
        "exclude": r"^es(/|$)",
        "product_pattern": r"^products(/|$)",
        "category_segment": 1,
    },
    "vermeer.com": {
        # Product pages are /na/<category>/<model>; found by keywords.
        "base_url": "https://www.vermeer.com/na/",
    },
    "na.develon-ce.com": {
        "base_url": "https://na.develon-ce.com/en/",
        "product_pattern": r"^construction-equipment(/|$)",
        "category_segment": 1,
    },
    "jlg.com": {
        # robots.txt lists 20 country sitemaps; /en/ is the English (US) section.
        # directaccess is a parts-ordering portal, not product information.
        "base_url": "https://www.jlg.com/en/",
        "exclude": r"^directaccess(/|$)",
        "product_pattern": r"^equipment(/|$)",
        "category_segment": 1,
    },
    "manitowoc.com": {
        # English pages have no language prefix; other languages are under /de/, /fr/, ...
        # Products are grouped by crane brand: /grove/..., /potain/..., /national-crane/...
        "base_url": "https://www.manitowoc.com/",
        "exclude": r"^(de|fr|es|pt|it|ru|ko|zh)(/|$)|^find-a-dealer(/|$)",
        "product_pattern": r"^(grove|potain|national-crane|manitowoc|krupp|shuttlelift)(/|$)",
        "category_segment": 1,
    },
    "toro.com": {
        # The homepage redirects to /en; the sitemap is a plain text list of addresses.
        "base_url": "https://www.toro.com/en/",
    },
    "ditchwitch.com": {
        # Most sitemap entries are uploaded images and files under /wp-content/.
        "base_url": "https://www.ditchwitch.com/",
        "exclude": r"^wp-content(/|$)",
        "product_pattern": r"^(trenchers|vacuum-excavation|directional-drills|stand-on-skid-steers|"
                           r"trenchless|hdd-tooling|vacs)(/|$)",
        "category_segment": 0,
    },
}
