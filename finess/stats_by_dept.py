import polars as pl


def pivot_finess_data(finess_data: pl.DataFrame) -> pl.DataFrame:
    """
    Pivots the finess data to long format.
    So we can have one row per type of bed by department.
    """
    return finess_data.unpivot(
        on=["mco", "ssr", "psy"],
        variable_name="type_lit",
        value_name="nb_etablissements",
        index=[
            "finess",
            "rs",
            "dept",
            "forme_juridique",
            "geoloc_4326_lat",
            "geoloc_4326_long",
        ],
    )


def stats_type_lit_by_dept(finess_data: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the number of hospitals by type of bed by department.
    """
    pivoted_finess_data = pivot_finess_data(finess_data)

    return (
        pivoted_finess_data.group_by(pl.col(["type_lit", "dept"]))
        .agg(value=pl.col("nb_etablissements").sum())
        .with_columns(type_lit=pl.col("type_lit").str.to_uppercase())
        .with_columns(indicator=pl.lit("nb_etablissements"))
    )
