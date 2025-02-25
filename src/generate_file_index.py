# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests",
#   "polars",
# ]
# ///
import polars as pl
import requests

data = (
    "673_2023_02_13_proteinAggregates_starvation_2_0_twice_ura7ha_ura7hr_ura8_ura8ha_ura8hr_00",
    "19129_2020_09_06_DownUpshift_2_0_2_glu_ura_mig1msn2_phluorin_00",
    "19144_2020_09_07_DownUpshift_2_0_2_glu_ura_mig1msn2_phluorin_secondRound_00",
    "19169_2020_09_09_downUpshift_2_0_2_glu_ura8_phl_mig1_phl_msn2_03",
    "19203_2020_09_30_downUpshift_twice_2_0_2_glu_ura8_ura8h360a_ura8h360r_00",
    "19232_2020_10_02_downUpshift_twice_2_0_2_glu_ura8_phluorinMsn2_phluorinMig1_01",
    "19307_2020_10_22_downUpshift_2_01_2_glucose_dual_pH__dot6_nrg1_tod6__00",
    "19310_2020_10_22_downUpshift_2_0_2_glu_dual_phluorin__glt1_psa1_ura7__thrice_00",
    "19311_2020_10_23_downUpshift_2_0_2_glu_dual_phluorin__glt1_psa1_ura7__twice__04",
    "19328_2020_10_31_downUpshift_four_2_0_2_glu_dual_phl__glt1_ura8_ura8__00",
    "19447_2020_11_18_downUpshift_2_0_2_glu_gcd2_gcd6_gcd7__02",
    "918_2023_02_28_starve_2_0_2_0_ura7ha_ura7hr_ura8_ura8ha_ura8hr_00",
    "921_2023_03_01_aggregates_starve_twice_glu_2_0_gcd2_gcd6_gcd7_gcn3_sui2_00",
    "16545_2019_07_16_aggregates_CTP_switch_2_0glu_0_0glu_URA7young_URA8young_URA8old_secondRun_01",
    "1238_2023_03_19_starve_twice_glu_2_0_2_0_ura7ha_ura7hr_ura8_ura8ha_ura8hr_00",
    "1100_2023_03_12_aggregates_downUpshift_glu_2_0_twice_gcd2_gcd6_gcn3_gcd7_sui2_00",
)


results = dict(
    expt_id=[],
    dataset=[],
    part=[],
    url=[],
    title=[],
    filename=[],
    md5=[],
    size=[],
    name=[],
    is_meta=[],
)
for dataset_name in data:
    response = requests.get(
        "https://zenodo.org/api/records",
        params={
            "q": dataset_name,
            "access_token": "",
        },
    )
    response_json = response.json()

    for dataset in response_json["hits"]["hits"]:
        for file in dataset["files"]:
            title = dataset["title"]
            url = file["links"]["self"]
            filename = url.split("/")[-2]
            results["url"].append(url)
            results["dataset"].append(dataset_name)
            results["title"].append(title)
            results["part"].append(int(title[:-1].split(" ")[-1]))
            results["filename"].append(filename)
            results["expt_id"].append(title.split("_")[0])
            results["md5"].append(file["checksum"])
            results["size"].append(file["size"])
            if filename.endswith("txt"):
                results["name"].append("")
                results["is_meta"].append(True)
            else:
                results["name"].append(filename.split(".")[0])
                results["is_meta"].append(False)

df = pl.from_dict(results).sort(by=("title", "filename"))
df.write_csv("index.csv")
