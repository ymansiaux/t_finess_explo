from finess import cleaning


def main():
    print("Hello from finesspy!")
    t_finess_clean = cleaning.clean_finess_data(
        csv_path="data/t-finess.csv",
        keep_only_mco_ssr_psy=True,
    )
    print(t_finess_clean)

    t_finess_geo = cleaning.finess_data_as_geodataframe(t_finess_clean)
    print(t_finess_geo)


if __name__ == "__main__":
    main()
