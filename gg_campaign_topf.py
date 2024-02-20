import argparse
import sys
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

import pandas as pd
from pathlib import Path 
from datetime import date, timedelta
today = date.today()

d1 = today - timedelta(days=280)
d2 = today - timedelta(days=1)

print("d1 =", d1)
print("d2 =", d2)

local_path = "your file loc"
export_path = "your ecport folder"
clt_path = "GoogleAdsClient file ymal locates"
customer_id = "your customer id find on Ads Words account"

def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          segments.date,  
          campaign.id,  
          campaign.name,
          metrics.clicks,
          metrics.impressions,
          metrics.average_cpc,
          metrics.ctr,
          campaign.optimization_score,
          campaign.target_cpa.target_cpa_micros,
          campaign.bidding_strategy_type,
          metrics.conversions, 
          metrics.conversions_value,
          metrics.cost_per_conversion,
          campaign.app_campaign_setting.bidding_strategy_goal_type,
          metrics.cost_micros,
          campaign.advertising_channel_type,
          metrics.average_time_on_site,
          campaign.status,
          campaign.maximize_conversion_value.target_roas,
          campaign_budget.recommended_budget_estimated_change_weekly_interactions,
          campaign_budget.amount_micros
        FROM 
            campaign
        WHERE metrics.clicks > 1
      AND segments.date BETWEEN '{0}' AND '{1}'
      AND campaign.advertising_channel_type = PERFORMANCE_MAX
        """.format(d1.strftime("%Y-%m-%d"), d2.strftime("%Y-%m-%d"))

    # Issues a search request using streaming.
    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    all_data = []

    for batch in stream:    
        for row in batch.results:
            single_row = dict()
            single_row["date"] = row.segments.date
            single_row["campaign_id"] = row.campaign.id
            single_row["campaign_name"] = row.campaign.name
            single_row["clicks"] = row.metrics.clicks
            single_row["impressions"] = row.metrics.impressions
            single_row["cpc"] = row.metrics.average_cpc
            single_row["ctr"] = row.metrics.ctr
            single_row["opt_score"] = row.campaign.optimization_score   
            single_row["tar_cpa"] = row.campaign.target_cpa.target_cpa_micros
            single_row["bid_strategy_type"] = row.campaign.bidding_strategy_type
            single_row["conversions"] = row.metrics.conversions
            single_row["conversions_value"] = row.metrics.conversions_value
            single_row["cost_per_conversion"] = row.metrics.cost_per_conversion
            single_row["app_objective"] = row.campaign.app_campaign_setting.bidding_strategy_goal_type
            single_row["spend"] = row.metrics.cost_micros
            #single_row["Skad_conversions"] = row.metrics.sk_ad_network_conversions  #metrics.sk_ad_network_conversions,
            single_row["channel type"] = row.campaign.advertising_channel_type
            single_row["time_on_app"] = row.metrics.average_time_on_site
            single_row["status"] = row.campaign.status
            single_row["tRoas_Value"] = row.campaign.maximize_conversion_value.target_roas
            single_row["Change_budget"] = row.campaign_budget.recommended_budget_estimated_change_weekly_interactions
            single_row["budget_cap"] = row.campaign_budget.amount_micros
            all_data.append(single_row)
            #print(all_data)
            df = pd.DataFrame(all_data)
            #print(df)
            #import os
            #path = export_path + "/"
            #output_file = os.path.join(export_path,'pmax.csv')
            output_file = os.path.join(export_path,'pmax_n.csv')
            df.to_csv(output_file, index=False)

#main(googleads_client,customer_id)
if __name__ == "__main__":
    googleads_client = GoogleAdsClient.load_from_storage(clt_path + "/google-ads.yaml")
    main(googleads_client,"customer_id") ### here is the customer ID

'''
if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    
    googleads_client = GoogleAdsClient.load_from_storage(local_path + "/google-ads.yaml")

    parser = argparse.ArgumentParser(
        description="Lists all campaigns for specified customer."
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=False,
        help="The Google Ads customer ID.",
    )
    args = parser.parse_args()

    try:
        main(googleads_client, args.customer_id)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)         
'''