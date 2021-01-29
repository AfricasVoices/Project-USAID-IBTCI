import argparse
import csv
import glob
import itertools
import shutil
from collections import OrderedDict
import sys

import geopandas
import matplotlib.pyplot as plt
from core_data_modules.analysis import AnalysisConfiguration, engagement_counts, theme_distributions, \
    repeat_participations, sample_messages
from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

from src import AnalysisUtils
from configuration.code_schemes import  CodeSchemes
from src.lib.configuration_objects import CodingModes
from src.mapping_utils import MappingUtils
from src.lib.pipeline_configuration import PipelineConfiguration

log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs automated analysis over the outputs produced by "
                                                 "`generate_outputs.py`, and optionally uploads the outputs to Drive.")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("engagement_metrics_input_dir", metavar="engagement-metrics-input-dir",
                        help="Path to a directory containing existing analysis generated earlier in the pipeline, to "
                             "be copied straight-through to the automated-analysis-output-dir")
    parser.add_argument("automated_analysis_output_dir", metavar="automated-analysis-output-dir",
                        help="Directory to write the automated analysis outputs to")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    engagement_metrics_input_dir = args.engagement_metrics_input_dir
    automated_analysis_output_dir = args.automated_analysis_output_dir

    IOUtils.ensure_dirs_exist(automated_analysis_output_dir)
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/regions")
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/districts")
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/mogadishu")

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    sys.setrecursionlimit(30000)
    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        for i in range (len(messages)):
            messages[i] = dict(messages[i].items())
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        for i in range (len(individuals)):
            individuals[i] = dict(individuals[i].items())
    log.info(f"Loaded {len(individuals)} individuals")

    log.info("Copying existing engagement metrics through to the analysis folder...")
    for path in glob.glob(f"{engagement_metrics_input_dir}/*"):
        log.info(f"Copying {path} -> {automated_analysis_output_dir}")
        shutil.copy(path, f"{automated_analysis_output_dir}")

    log.info(f"Computing the estimated engagement types")
    stats = []
    for (plan, rqa_plan) in zip(PipelineConfiguration.ENGAGEMENT_CODING_PLANS, PipelineConfiguration.RQA_CODING_PLANS):
        opt_ins = AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, [rqa_plan])
        relevant = AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, [rqa_plan])

        for cc in plan.coding_configurations:
            assert cc.coding_mode == CodingModes.SINGLE

            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue

                stats.append({
                    "Episode": plan.dataset_name,
                    "Estimated Engagement Type": code.string_value,
                    "Messages with Opt-Ins": len([msg for msg in opt_ins if msg[cc.coded_field]["CodeID"] == code.code_id]),
                    "Relevant Messages": len([msg for msg in relevant if msg[cc.coded_field]["CodeID"] == code.code_id])
                })

    with open(f"{automated_analysis_output_dir}/estimated_engagement_types.csv", "w") as f:
        headers = ["Episode", "Estimated Engagement Type", "Messages with Opt-Ins", "Relevant Messages"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in stats:
            writer.writerow(row)

    def coding_plans_to_analysis_configurations(coding_plans):
        analysis_configurations = []
        for plan in coding_plans:
            for cc in plan.coding_configurations:
                if not cc.include_in_theme_distribution:
                    continue

                analysis_configurations.append(
                    AnalysisConfiguration(cc.analysis_file_key, plan.raw_field, cc.coded_field, cc.code_scheme)
                )
        return analysis_configurations

    # Engagement Counts
    log.info("Computing engagement counts...")
    with open(f"{automated_analysis_output_dir}/engagement_counts.csv", "w") as f:
        engagement_counts.export_engagement_counts_csv(
            messages, individuals, CONSENT_WITHDRAWN_KEY,
            coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
            f
        )

    log.info("Computing participation frequencies...")
    with open(f"{automated_analysis_output_dir}/participation_frequencies.csv", "w") as f:
        repeat_participations.export_repeat_participations_csv(
            individuals, CONSENT_WITHDRAWN_KEY,
            coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
            f
        )

    log.info("Computing theme distributions...")
    with open(f"{automated_analysis_output_dir}/theme_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            individuals, CONSENT_WITHDRAWN_KEY,
            coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
            coding_plans_to_analysis_configurations(PipelineConfiguration.SURVEY_CODING_PLANS),
            f
        )

    log.info("Computing demographic distributions...")
    with open(f"{automated_analysis_output_dir}/demographic_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            individuals, CONSENT_WITHDRAWN_KEY,
            coding_plans_to_analysis_configurations(PipelineConfiguration.DEMOG_CODING_PLANS),
            [],
            f
        )

    log.info("Exporting up to 100 sample messages for each RQA code...")
    with open(f"{automated_analysis_output_dir}/sample_messages.csv", "w") as f:
        sample_messages.export_sample_messages_csv(
            messages, CONSENT_WITHDRAWN_KEY,
            coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
            f,
            limit_per_code=100
        )

    log.info("Computing loyalty...")
    loyalty = OrderedDict()
    dataset_names = [plan.dataset_name for plan in PipelineConfiguration.RQA_CODING_PLANS]
    normalise_episodes = lambda episodes: tuple(sorted(list(set(episodes))))
    for r in range(1, len(dataset_names) + 1):
        for episodes in itertools.combinations(dataset_names, r):
            loyalty[normalise_episodes(episodes)] = 0

    for ind in individuals:
        if AnalysisUtils.withdrew_consent(ind, CONSENT_WITHDRAWN_KEY):
            continue

        participated_episodes = set()
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if AnalysisUtils.responded(ind, plan):
                participated_episodes.add(plan.dataset_name)
        loyalty[normalise_episodes(participated_episodes)] += 1

    with open(f"{automated_analysis_output_dir}/loyalty.csv", "w") as f:
        headers = ["Episode Combination", "Participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for episode_combination, participants in loyalty.items():
            writer.writerow({
                "Episode Combination": ", ".join(episode_combination),
                "Participants": participants
            })

    if pipeline_configuration.pipeline_name == "USAID-IBTCI-Facebook":
        # Only the total engagement counts make sense for now, so don't attempt to apply any of the other standard
        # analysis to the Facebook data.
        exit(0)

    # Temporary adaptations to keep the existing mapping code working.
    # TODO: Tidy up the mapping code.
    demographic_distributions = theme_distributions.compute_theme_distributions(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.DEMOG_CODING_PLANS),
        []
    )

    episodes = theme_distributions.compute_theme_distributions(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        coding_plans_to_analysis_configurations(PipelineConfiguration.DEMOG_CODING_PLANS)
    )

    log.info("Loading the Somali regions geojson...")
    regions_map = geopandas.read_file("geojson/somalia_regions.geojson")

    log.info("Generating a map of per-region participation for the season")
    region_frequencies = dict()
    for code in CodeSchemes.SOMALIA_REGION.codes:
        if code.code_type == CodeTypes.NORMAL:
            region_frequencies[code.string_value] = demographic_distributions["region"][code.string_value]["Total Participants"]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", region_frequencies,
                                    label_position_columns=("ADM1_LX", "ADM1_LY"),
                                    callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
    plt.savefig(f"{automated_analysis_output_dir}/maps/regions/regions_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close()

    if pipeline_configuration.automated_analysis.generate_region_theme_distribution_maps:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            episode = episodes[plan.dataset_name]

            for cc in plan.coding_configurations:
                # Plot a map of the total relevant participants for this coding configuration.
                rqa_total_region_frequencies = dict()
                for region_code in CodeSchemes.SOMALIA_REGION.codes:
                    if region_code.code_type == CodeTypes.NORMAL:
                        rqa_total_region_frequencies[region_code.string_value] = \
                            episode["Total Relevant Participants"][f"region:{region_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", rqa_total_region_frequencies,
                                                label_position_columns=("ADM1_LX", "ADM1_LY"),
                                                callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
                plt.savefig(f"{automated_analysis_output_dir}/maps/regions/region_{cc.analysis_file_key}_1_total_relevant.png",
                            dpi=1200, bbox_inches="tight")
                plt.close()

                # Plot maps of each of the normal themes for this coding configuration.
                map_index = 2  # (index 1 was used in the total relevant map's filename).
                for code in cc.code_scheme.codes:
                    if code.code_type != CodeTypes.NORMAL:
                        continue

                    log.info(f"Generating a map of per-region participation for {code.string_value}...")
                    demographic_counts = episode[code.string_value]

                    theme_region_frequencies = dict()
                    for region_code in CodeSchemes.SOMALIA_REGION.codes:
                        if region_code.code_type == CodeTypes.NORMAL:
                            theme_region_frequencies[region_code.string_value] = \
                                demographic_counts[f"region:{region_code.string_value}"]

                    fig, ax = plt.subplots()
                    MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", theme_region_frequencies,
                                                    label_position_columns=("ADM1_LX", "ADM1_LY"),
                                                    callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
                    plt.savefig(f"{automated_analysis_output_dir}/maps/regions/region_{cc.analysis_file_key}_{map_index}_{code.string_value}.png",
                                dpi=1200, bbox_inches="tight")
                    plt.close()

                    map_index += 1
    log.info("Skipping generating a map of per-region theme participation because "
             "`generate_region_theme_distribution_maps` is set to False")

    log.info("Loading the Somalia districts geojson...")
    districts_map = geopandas.read_file("geojson/somalia_districts.geojson")

    log.info("Generating a map of per-district participation for the season")
    district_frequencies = dict()
    for code in CodeSchemes.SOMALIA_DISTRICT.codes:
        if code.code_type == CodeTypes.NORMAL:
            district_frequencies[code.string_value] = demographic_distributions["district"][code.string_value]["Total Participants"]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", district_frequencies, ax=ax)
    plt.savefig(f"{automated_analysis_output_dir}/maps/districts/district_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    if pipeline_configuration.automated_analysis.generate_district_theme_distribution_maps:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            episode = episodes[plan.dataset_name]

            for cc in plan.coding_configurations:
                # Plot a map of the total relevant participants for this coding configuration.
                rqa_total_district_frequencies = dict()
                for district_code in CodeSchemes.SOMALIA_DISTRICT.codes:
                    if district_code.code_type == CodeTypes.NORMAL:
                        rqa_total_district_frequencies[district_code.string_value] = \
                            episode["Total Relevant Participants"][f"district:{district_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", rqa_total_district_frequencies, ax=ax)
                plt.savefig(f"{automated_analysis_output_dir}/maps/districts/district_{cc.analysis_file_key}_1_total_relevant.png",
                            dpi=1200, bbox_inches="tight")
                plt.close(fig)

                # Plot maps of each of the normal themes for this coding configuration.
                map_index = 2  # (index 1 was used in the total relevant map's filename).
                for code in cc.code_scheme.codes:
                    if code.code_type != CodeTypes.NORMAL:
                        continue

                    log.info(f"Generating a map of per-district participation for {code.string_value}...")
                    demographic_counts = episode[code.string_value]

                    theme_district_frequencies = dict()
                    for district_code in CodeSchemes.SOMALIA_DISTRICT.codes:
                        if district_code.code_type == CodeTypes.NORMAL:
                            theme_district_frequencies[district_code.string_value] = \
                                demographic_counts[f"district:{district_code.string_value}"]

                    fig, ax = plt.subplots()
                    MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", theme_district_frequencies, ax=ax)
                    plt.savefig(
                        f"{automated_analysis_output_dir}/maps/districts/district_{cc.analysis_file_key}_{map_index}_{code.string_value}.png",
                        dpi=1200, bbox_inches="tight")
                    plt.close(fig)

                    map_index += 1
    log.info("Skipping generating a map of per-district theme participation because "
             "`generate_district_theme_distribution_maps` is set to False")

    log.info("Loading the Mogadishu sub-district geojson...")
    mogadishu_map = geopandas.read_file("geojson/mogadishu_sub_districts.geojson")

    log.info("Generating a map of Mogadishu participation for the season...")
    mogadishu_frequencies = dict()
    for code in CodeSchemes.MOGADISHU_SUB_DISTRICT.codes:
        if code.code_type == CodeTypes.NORMAL:
            mogadishu_frequencies[code.string_value] = demographic_distributions["mogadishu_sub_district"][
                code.string_value]["Total Participants"]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(mogadishu_map, "ADM3_AVF", mogadishu_frequencies, ax=ax,
                                    label_position_columns=("ADM3_LX", "ADM3_LY"))
    plt.savefig(f"{automated_analysis_output_dir}/maps/mogadishu/mogadishu_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    if pipeline_configuration.automated_analysis.generate_mogadishu_theme_distribution_maps:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            episode = episodes[plan.dataset_name]

            for cc in plan.coding_configurations:
                # Plot a map of the total relevant participants for this coding configuration.
                rqa_total_mogadishu_frequencies = dict()
                for sub_district_code in CodeSchemes.MOGADISHU_SUB_DISTRICT.codes:
                    if sub_district_code.code_type == CodeTypes.NORMAL:
                        rqa_total_mogadishu_frequencies[sub_district_code.string_value] = \
                            episode["Total Relevant Participants"][f"mogadishu_sub_district:{sub_district_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(mogadishu_map, "ADM3_AVF", rqa_total_mogadishu_frequencies, ax=ax,
                                                label_position_columns=("ADM3_LX", "ADM3_LY"))
                plt.savefig(f"{automated_analysis_output_dir}/maps/mogadishu/mogadishu_{cc.analysis_file_key}_1_total_relevant.png",
                            dpi=1200, bbox_inches="tight")
                plt.close(fig)

                # Plot maps of each of the normal themes for this coding configuration.
                map_index = 2  # (index 1 was used in the total relevant map's filename).
                for code in cc.code_scheme.codes:
                    if code.code_type != CodeTypes.NORMAL:
                        continue

                    log.info(f"Generating a map of Mogadishu participation for {code.string_value}...")
                    demographic_counts = episode[code.string_value]

                    mogadishu_theme_frequencies = dict()
                    for sub_district_code in CodeSchemes.MOGADISHU_SUB_DISTRICT.codes:
                        if sub_district_code.code_type == CodeTypes.NORMAL:
                            mogadishu_theme_frequencies[sub_district_code.string_value] = \
                                demographic_counts[f"mogadishu_sub_district:{sub_district_code.string_value}"]

                    fig, ax = plt.subplots()
                    MappingUtils.plot_frequency_map(mogadishu_map, "ADM3_AVF", mogadishu_theme_frequencies, ax=ax,
                                                    label_position_columns=("ADM3_LX", "ADM3_LY"))
                    plt.savefig(
                        f"{automated_analysis_output_dir}/maps/mogadishu/mogadishu_{cc.analysis_file_key}_{map_index}_{code.string_value}.png",
                        dpi=1200, bbox_inches="tight")
                    plt.close(fig)

                    map_index += 1
    log.info("Skipping generating a map of mogadishu theme participation because "
             "`generate_mogadishu_theme_distribution_maps` is set to False")

    log.info("automated analysis python script complete")
