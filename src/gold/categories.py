"""
Standard equipment categories for the gold layer.

Each company names its product sections differently ("dozer", "dozers",
"crawler-dozers", "construction-equipment/dozers"). build_gold.py matches every
product page address (silver.site_urls.relative_path, lowercase) against
CATEGORY_RULES in order; the first match gives its standard category and
segment. Order matters: specific rules come before general ones (attachments
before backhoes, forestry harvesters before crop harvesting, mini excavators
before excavators).

Pages matching NOT_PRODUCT are dropped before matching (compare tools, finance,
parts, offers, legal pages listed under product sections). Product pages that
match no rule are left out of the category tables; build_gold.py prints the
most common ones so new rules can be added here.
"""

NOT_PRODUCT = (
    r"compare|comparison|spec-check|testing-page|legal|financ|(^|/)parts(/|$)|offers?(/|$)|"
    r"promotion|warranty|build-and-price|inventory|(^|/)used|register-used|prior-models|non-current|"
    r"brochure|dealer|find-a|request-a|quote|demo(/|$)|government|military|"
    r"power-gard|protection-plan|digital-products|rental-resources|we-drill|(^|/)features(/|$)"
)

# (standard category, segment, regex on the page address)
CATEGORY_RULES = [
    # Checked first: these words also appear inside machine category names.
    # products-and-solutions/loaders: John Deere's front loaders for tractors.
    ("Attachments", "Attachments", r"attachment|accessor|(^|[/-])(blades?|buckets?)(/|$)|products-and-solutions/loaders"),
    ("Engines & Powertrain", "Other", r"engine|drivetrain|powertrain|agco-power|pump-drive"),
    ("Hay & Forage", "Agriculture", r"vertical-mixer|feed-mixer"),  # livestock feed mixers, not concrete

    ("Forestry & Tree Care", "Forestry", r"forest|feller|forwarder|skidder|log-loader|logging|timber|"
                                         r"harvester-head|chipper|stump|tree-care|brush|horizontal-grinder|tub-grinder"),
    ("Mining Equipment", "Mining", r"mining|shovel|dragline|longwall|continuous-miner|blasthole|room-and-pillar|"
                                   r"bolter|underground|shaft-sinking|entry-development|hard-rock|industrial-minerals|crush|"
                                   r"haul-truck|surface-drill"),
    ("Drills & Trenchers", "Construction", r"directional-drill|trencher|vacuum-excavat|horizontal-directional"),

    ("Mini Excavators", "Construction", r"mini-excavator|compact-excavator|mini-ex(/|-|$)"),
    ("Excavators", "Construction", r"excavator|high-reach"),
    ("Skid Steer & Track Loaders", "Construction", r"skid-steer|track-loader|compact-track|mini-loader|"
                                                   r"mini-track|compact-loader|articulated-loader|small-articulated"),
    ("Backhoe Loaders", "Construction", r"backhoe|tractor-loader"),
    ("Wheel Loaders", "Construction", r"wheel-loader"),
    ("Dozers", "Construction", r"dozer"),
    ("Motor Graders", "Construction", r"motor-grader"),
    ("Haulers & Dump Trucks", "Construction", r"hauler|dump-truck|dumper|dumpster|(^|/)trucks(/|$)"),
    # Tillage rollers are not compaction equipment.
    ("Compaction", "Construction", r"^(?!.*(tillage|seed|planter)).*(compaction|compactor|roller|vibratory|rammer)"),
    ("Aerial Work Platforms", "Construction", r"aerial|boom-lift|articulated-boom|telescopic-boom|scissor|digger-derrick"),
    ("Concrete & Paving", "Construction", r"concrete|paver|paving|asphalt|mixer|road-build|(^|/)(advance|bid-well)(/|$)"),
    ("Light & Power Equipment", "Construction", r"light-tower|lighting|generator|power-supply|portable-power|"
                                                r"compressor|pump|heater"),

    # Before telehandlers: New Holland lists tractors under "tractors-telehandlers".
    ("Tractors", "Agriculture", r"tractor"),
    ("Telehandlers & Material Handling", "Construction", r"telehandler|telescopic-handler|telescopic-loader|loadall|"
                                                          r"material-handl|forklift|reach-stacker|warehouse|"
                                                          r"(^|/)marco(/|$)"),
    ("Combines & Harvesting", "Agriculture", r"combine|harvest|header|cotton|grape|sugarcane"),
    ("Hay & Forage", "Agriculture", r"(^|[/-])hay|forage|baler|bale|rake|tedder|wrapper|mower-conditioner|windrower"),
    ("Seeding & Tillage", "Agriculture", r"seed|planter|tillage|cultivat|plow|disk"),
    ("Sprayers & Crop Care", "Agriculture", r"spray|crop-care|nutrient|applicator|spreader"),
    ("Precision Technology", "Agriculture", r"precision|guidance|gps|technology-solutions|telematics"),

    ("Mowers & Turf", "Turf & Utility", r"mower|turf|lawn|golf|zero-turn|landscap|aerator|cutter|shredder"),
    ("Utility Vehicles", "Turf & Utility", r"utility-vehicle|gator|(^|[/-])rtv|(^|[/-])utv|xuv|crossover|toolcat"),
]

# Category order for the dashboard heatmap: grouped by segment.
CATEGORY_ORDER = [
    "Mini Excavators", "Excavators", "Skid Steer & Track Loaders", "Backhoe Loaders", "Wheel Loaders",
    "Dozers", "Motor Graders", "Haulers & Dump Trucks", "Telehandlers & Material Handling", "Compaction",
    "Aerial Work Platforms", "Concrete & Paving", "Drills & Trenchers", "Light & Power Equipment",
    "Tractors", "Combines & Harvesting", "Hay & Forage", "Seeding & Tillage", "Sprayers & Crop Care",
    "Precision Technology", "Mowers & Turf", "Utility Vehicles", "Forestry & Tree Care", "Mining Equipment",
    "Engines & Powertrain", "Attachments",
]

# Product pages about electric or battery machines (electric_products table).
ELECTRIC = r"electric|battery|zero-emission|(^|[/-])ev(/|-|$)"
# Landing pages that only list electric machines; not products themselves.
ELECTRIC_LANDING = r"^(electric|battery|zero-emission)[a-z-]*-(machines|equipment|products|solutions|range|lineup|portfolio)$"
