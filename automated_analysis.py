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
from core_data_modules.analysis.participation_maps import export_participation_maps
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

from configuration.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingModes
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

    # Export traffic analysis
    if pipeline_configuration.automated_analysis.traffic_labels is not None:
        traffic_analysis = []
        for traffic_label in pipeline_configuration.automated_analysis.traffic_labels:
            opt_in_messages = analysis_utils.filter_opt_ins(
                messages, CONSENT_WITHDRAWN_KEY,
                coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS)
            )

            opt_in_messages_in_time_range = [
                msg for msg in opt_in_messages
                if traffic_label.start_date <= isoparse(msg["sent_on"]) < traffic_label.end_date
            ]

            traffic_analysis.append({
                "Start Date": traffic_label.start_date.isoformat(),
                "End Date": traffic_label.end_date.isoformat(),
                "Label": traffic_label.label,
                "Messages with Opt-Ins": len(opt_in_messages_in_time_range),
                "Relevant Messages": len(
                    analysis_utils.filter_relevant(
                        opt_in_messages_in_time_range, CONSENT_WITHDRAWN_KEY,
                        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS)
                    )
                )
            })

        with open(f"{automated_analysis_output_dir}/traffic_analysis.csv", "w") as f:
            headers = ["Start Date", "End Date", "Label", "Messages with Opt-Ins", "Relevant Messages"]
            writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
            writer.writeheader()

            for row in traffic_analysis:
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
        f"{automated_analysis_output_dir}/maps/regions/regions_",
        export_by_theme=pipeline_configuration.automated_analysis.generate_region_theme_distribution_maps
    )

    log.info(f"Exporting participation maps for each Somalia district...")
    export_participation_maps(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        AnalysisConfiguration("district", "location_raw", "district_coded", CodeSchemes.SOMALIA_DISTRICT),
        somalia_mapper.export_somalia_district_frequencies_map,
        f"{automated_analysis_output_dir}/maps/districts/districts_",
        export_by_theme=pipeline_configuration.automated_analysis.generate_district_theme_distribution_maps
    )

    log.info(f"Exporting participation maps for each Mogadishu sub-district...")
    export_participation_maps(
        individuals, CONSENT_WITHDRAWN_KEY,
        coding_plans_to_analysis_configurations(PipelineConfiguration.RQA_CODING_PLANS),
        AnalysisConfiguration("mogadishu_sub_district", "location_raw", "mogadishu_sub_district_coded", CodeSchemes.MOGADISHU_SUB_DISTRICT),
        somalia_mapper.export_mogadishu_sub_district_frequencies_map,
        f"{automated_analysis_output_dir}/maps/mogadishu/mogadishu_",
        export_by_theme=pipeline_configuration.automated_analysis.generate_mogadishu_theme_distribution_maps
    )

    log.info("automated analysis python script complete")
