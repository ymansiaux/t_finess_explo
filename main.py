from finess import cleaning, stats_by_dept
import polars as pl


def main():
    print("Hello from finesspy!")
    t_finess_clean = cleaning.clean_finess_data(
        csv_path="data/t-finess.csv",
        keep_only_mco_ssr_psy=True,
    )
    print(t_finess_clean)

    t_finess_geo = cleaning.finess_data_as_geodataframe(t_finess_clean)
    print(t_finess_geo)

    stats_finess_by_dept = stats_by_dept.stats_type_lit_by_dept(t_finess_clean)
    print(stats_finess_by_dept)


if __name__ == "__main__":
    main()


# t_finess_long = t_finess_clean.unpivot(
#     on=["mco", "ssr", "psy"],
#     variable_name="type_lit",
#     value_name="nb_etablissements",
#     index=[
#         "finess",
#         "rs",
#         "dept",
#         "forme_juridique",
#         "geoloc_4326_lat",
#         "geoloc_4326_long",
#     ],
# )

# stats_finess = (
#     t_finess_long.group_by(pl.col(["type_lit", "dept"]))
#     .agg(value=pl.col("nb_etablissements").sum())
#     .with_columns(type_lit=pl.col("type_lit").str.to_uppercase())
#     .with_columns(indicator=pl.lit("nb_etablissements"))
# )


# import plotnine as p9

# plot = p9.ggplot(stats_finess) + p9.geom_col(
#     p9.aes(x="type_lit", y="value", fill="indicator")
# )
# plot.show()
