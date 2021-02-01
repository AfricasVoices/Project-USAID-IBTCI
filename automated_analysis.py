import argparse
import csv
import glob
import itertools
import shutil
from collections import OrderedDict
import sys

from core_data_modules.analysis import AnalysisConfiguration, engagement_counts, theme_distributions, \
    repeat_participations, sample_messages, analysis_utils
from core_data_modules.analysis.mapping import somalia_mapper
from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

from configuration.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingModes
from src.lib.pipeline_configuration import PipelineConfiguration

log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"


def export_participation_maps(individuals, consent_withdrawn_field, theme_configurations, region_configuration, mapper, file_prefix,
                              export_by_theme=True):
    # Export a map showing the total participations
    log.info(f"Exporting map to '{file_prefix}_total_participants.png'...")
    region_distributions = theme_distributions.compute_theme_distributions(
        individuals, consent_withdrawn_field,
        [region_configuration],
        []
    )[region_configuration.dataset_name]

    total_frequencies = dict()
    for region_code in [c for c in region_configuration.code_scheme.codes if c.code_type == CodeTypes.NORMAL]:
        total_frequencies[region_code.string_value] = region_distributions[region_code.string_value]["Total Participants"]

    mapper(total_frequencies, f"{file_prefix}_total_participants.png")

    if not export_by_theme:
        return

    # For each theme_configuration, export:
    #  1. A map showing the totals for individuals relevant to that episode.
    #  2. A map showing the totals for each theme
    themes = theme_distributions.compute_theme_distributions(
        individuals, consent_withdrawn_field,
        theme_configurations,
        [region_configuration]
    )

    for config in theme_configurations:
        log.info(f"Exporting map to '{file_prefix}_{config.dataset_name}_1_total_relevant.png'...")
        config_total_frequencies = dict()
        for region_code in [c for c in region_configuration.code_scheme.codes if c.code_type == CodeTypes.NORMAL]:
            config_total_frequencies[region_code.string_value] = themes[config.dataset_name]["Total Relevant Participants"][
                f"{region_configuration.dataset_name}:{region_code.string_value}"]

        mapper(config_total_frequencies, f"{file_prefix}_{config.dataset_name}_1_total_relevant.png")

        map_index = 2
        for theme in [c for c in config.code_scheme.codes if c.code_type == CodeTypes.NORMAL]:
            log.info(f"Exporting map to '{file_prefix}_{config.dataset_name}_{map_index}_{theme.string_value}.png'...")
            theme_frequencies = dict()
            for region_code in [c for c in region_configuration.code_scheme.codes if c.code_type == CodeTypes.NORMAL]:
                theme_frequencies[region_code.string_value] = themes[config.dataset_name][theme.string_value][
                    f"{region_configuration.dataset_name}:{region_code.string_value}"]

            mapper(theme_frequencies, f"{file_prefix}_{config.dataset_name}_{map_index}_{theme.string_value}.png")

            map_index += 1


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

    log.info("Computing repeat participations...")
    with open(f"{automated_analysis_output_dir}/repeat_participations.csv", "w") as f:
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

    log.info(f"Computing the estimated engagement types")
    stats = []
    for (plan, rqa_plan) in zip(PipelineConfiguration.ENGAGEMENT_CODING_PLANS, PipelineConfiguration.RQA_CODING_PLANS):
        opt_ins = analysis_utils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, coding_plans_to_analysis_configurations([rqa_plan]))
        relevant = analysis_utils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, coding_plans_to_analysis_configurations([rqa_plan]))

        for cc in plan.coding_configurations:
            assert cc.coding_mode == CodingModes.SINGLE

            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue

                stats.append({
                    "Episode": plan.dataset_name,
                    "Estimated Engagement Type": code.string_value,
                    "Messages with Opt-Ins": len(
                        [msg for msg in opt_ins if msg[cc.coded_field]["CodeID"] == code.code_id]),
                    "Relevant Messages": len([msg for msg in relevant if msg[cc.coded_field]["CodeID"] == code.code_id])
                })

    with open(f"{automated_analysis_output_dir}/estimated_engagement_types.csv", "w") as f:
        headers = ["Episode", "Estimated Engagement Type", "Messages with Opt-Ins", "Relevant Messages"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in stats:
            writer.writerow(row)

    log.info("Computing loyalty...")
    loyalty = OrderedDict()
    dataset_names = [plan.dataset_name for plan in PipelineConfiguration.RQA_CODING_PLANS]
    normalise_episodes = lambda episodes: tuple(sorted(list(set(episodes))))
    for r in range(1, len(dataset_names) + 1):
        for episodes in itertools.combinations(dataset_names, r):
            loyalty[normalise_episodes(episodes)] = 0

    for ind in individuals:
        if analysis_utils.withdrew_consent(ind, CONSENT_WITHDRAWN_KEY):
            continue

        participated_episodes = set()
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if analysis_utils.responded(ind, coding_plans_to_analysis_configurations([plan])[0]):
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

    log.info(f"Exporting participation maps for each Somalia region...")
    export_participation_maps(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        AnalysisConfiguration("region", "location_raw", "region_coded", CodeSchemes.SOMALIA_REGION),
        somalia_mapper.export_somalia_region_frequencies_map,
        f"{automated_analysis_output_dir}/maps/regions/regions",
        export_by_theme=pipeline_configuration.automated_analysis.generate_region_theme_distribution_maps
    )

    log.info(f"Exporting participation maps for each Somalia district...")
    export_participation_maps(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        AnalysisConfiguration("district", "location_raw", "district_coded", CodeSchemes.SOMALIA_DISTRICT),
        somalia_mapper.export_somalia_district_frequencies_map,
        f"{automated_analysis_output_dir}/maps/districts/districts",
        export_by_theme=pipeline_configuration.automated_analysis.generate_district_theme_distribution_maps
    )

    log.info(f"Exporting participation maps for each Mogadishu sub-district...")
    export_participation_maps(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        AnalysisConfiguration("mogadishu_sub_district", "location_raw", "mogadishu_sub_district_coded", CodeSchemes.MOGADISHU_SUB_DISTRICT),
        somalia_mapper.export_mogadishu_sub_district_frequencies_map,
        f"{automated_analysis_output_dir}/maps/mogadishu/mogadishu",
        export_by_theme=pipeline_configuration.automated_analysis.generate_mogadishu_theme_distribution_maps
    )

    log.info("automated analysis python script complete")
