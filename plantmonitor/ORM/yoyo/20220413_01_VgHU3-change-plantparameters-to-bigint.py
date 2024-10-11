"""
Change plantparameters to bigint
"""

from yoyo import step

__depends__ = {'20210920_02_kFUDk-add-inverter-strings-tables'}

steps = [
    step("""
        ALTER TABLE plantparameters
        ALTER COLUMN peak_power_w TYPE BIGINT,
        ALTER COLUMN nominal_power_w TYPE BIGINT,
        ALTER COLUMN target_monthly_energy_wh TYPE BIGINT,
        ALTER COLUMN historic_monthly_energy_wh TYPE BIGINT,
        ALTER COLUMN month_theoric_pr_cpercent TYPE BIGINT,
        ALTER COLUMN year_theoric_pr_cpercent TYPE BIGINT;
    """,
    """
        ALTER TABLE plantparameters
        ALTER COLUMN peak_power_w TYPE INTEGER,
        ALTER COLUMN nominal_power_w TYPE INTEGER,
        ALTER COLUMN target_monthly_energy_wh TYPE INTEGER,
        ALTER COLUMN historic_monthly_energy_wh TYPE INTEGER,
        ALTER COLUMN month_theoric_pr_cpercent TYPE INTEGER,
        ALTER COLUMN year_theoric_pr_cpercent TYPE INTEGER;
    """)
]
