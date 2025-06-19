import polars as pl
import geopandas as gpd
from typing import Optional
import geopandas as gpd


def clean_finess_data(
    csv_path: str,
    keep_only_mco_ssr_psy: bool = False,
    cols_to_keep: Optional[list[str]] = None,
) -> pl.DataFrame:
    """
    Clean the Finess data from the CSV file.
    """
    if cols_to_keep is None:
        cols_to_keep = [
            "finess",
            "dept",
            "rs",
            "mco",
            "ssr",
            "psy",
            "forme_juridique",
            "geoloc_4326_lat",
            "geoloc_4326_long",
        ]

    t_finess = pl.read_csv(csv_path, ignore_errors=True)

    t_finess_clean = (
        t_finess.filter(pl.col("etat") == "ACTUEL")
        .filter(pl.col("statut_jur_etat") == "O")
        .filter(pl.col("geoloc_legal_projection") == "L93_METROPOLE")
        .with_columns(
            mco=(pl.col("san_med") == "OUI")
            | (pl.col("san_chir") == "OUI")
            | (pl.col("san_obs") == "OUI")
        )
        .with_columns(ssr=(pl.col("san_smr") == "OUI"))
        .with_columns(psy=(pl.col("san_psy") == "OUI"))
        .with_columns(
            forme_juridique=pl.when(
                pl.col("statut_jur_niv1_lib").str.to_lowercase().str.contains("public")
            )
            .then(pl.lit("public"))
            .when(
                pl.col("statut_jur_niv1_lib").str.to_lowercase().str.contains("privé")
            )
            .then(pl.lit("privé"))
            .otherwise(None)
        )
        .with_columns(dept=pl.col("finess").cast(pl.Utf8).str.slice(0, 2))
    )

    if keep_only_mco_ssr_psy:
        t_finess_clean = t_finess_clean.filter(
            pl.col("mco") | pl.col("ssr") | pl.col("psy")
        )

    return t_finess_clean.select(pl.col(cols_to_keep))


def finess_data_as_geodataframe(t_finess_clean: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert the Finess data to a GeoDataFrame.
    """
    geo_data = gpd.GeoDataFrame(
        t_finess_clean.to_pandas(),
        geometry=gpd.points_from_xy(
            t_finess_clean.select(pl.col("geoloc_4326_long")).to_pandas()[
                "geoloc_4326_long"
            ],
            t_finess_clean.select(pl.col("geoloc_4326_lat")).to_pandas()[
                "geoloc_4326_lat"
            ],
        ),
        crs="EPSG:4326",
    )
    return geo_data
