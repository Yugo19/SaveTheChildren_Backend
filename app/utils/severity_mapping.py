"""
Severity mapping utility for deriving severity levels from abuse types.
Since the data doesn't have a native 'severity' field, we derive it from abuse_type.
"""


def get_severity_mapping():
    """Get the mapping of abuse types to severity levels"""
    return {
        "high": [
            "Defilement", "Child radicalization", "Trafficked child", 
            "FGM", "Sexual abuse", "Sexual assault", "Sexual Exploitation and abuse",
            "Physical abuse / Violence", "Abduction", "Parental child abduction",
            "Sodomy", "Incest", "Unlawful confinement"
        ],
        "medium": [
            "Child Marriage", "Child pregnancy", "Child Labour", 
            "Neglect", "Emotional Abuse", "Child Delinquency",
            "Child offender", "Custody", "Children on the streets",
            "Drug and Substance Abuse", "Online Abuse", "Harmful cultural practice"
        ],
        "low": [
            "Child out of school", "Child truancy", "Child Mother",
            "Child with disability", "Child Affected by HIV/AIDS",
            "Child headed household", "Child of imprisoned parent (s)",
            "Abandoned", "CSSM", "Destitution", "Disputed paternity",
            "Inheritance / Succession", "Internally displaced child",
            "Lost and found children", "Mother (Relative) Offer",
            "Orphaned Child", "Refugee Children", "Registration",
            "Sick Child (Chronic Illness)", "not_resolved"
        ]
    }


def get_severity_aggregation_stage():
    """
    Get MongoDB aggregation stage to derive severity from abuse_type.
    Use this in $addFields stage of aggregation pipelines.
    
    Returns:
        dict: MongoDB aggregation expression for derived_severity field
    """
    severity_mapping = get_severity_mapping()
    
    return {
        "$switch": {
            "branches": [
                {
                    "case": {"$in": ["$abuse_type", severity_mapping["high"]]},
                    "then": "high"
                },
                {
                    "case": {"$in": ["$abuse_type", severity_mapping["medium"]]},
                    "then": "medium"
                },
                {
                    "case": {"$in": ["$abuse_type", severity_mapping["low"]]},
                    "then": "low"
                }
            ],
            "default": "unknown"
        }
    }
