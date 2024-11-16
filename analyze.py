#!/usr/bin/env python3
from typing import Dict
import pathlib
import pytrie

import pprint
import pandas
from prefixspan import PrefixSpan
from google.cloud import bigquery, bigquery_storage


# Function to check if a pattern is a subsequence of a sequence
def is_subsequence(pattern, sequence):
    it = iter(sequence)  # When it is an iterator, all respects order of occurrence
    return all(item in it for item in pattern)


def uniquest_prefix_in_prowjobs(prowjob_df: pandas.DataFrame, attr: str) -> Dict[str, str]:
    """
    Given a dataframe with a column prowjob_build_id and an attribute name (e.g. 'e_pod'), return the
    mapping of e_pod names to their most unique prefix found in more than one prowjob. If the name
    exists in only one prowjob, the full name will be mapped.
    """
    trie = pytrie.StringTrie()
    for _, row in prowjob_df.iterrows():
        e_pod = row[attr]
        if not e_pod:
            continue
        split = e_pod.split('-')
        for i in range(1, len(split)+1):
            entry = '-'.join(split[0:i])
            if entry not in trie:
                trie[entry] = set()
            trie[entry].add(row['prowjob_build_id'])

    name_mapping: Dict[str, str] = dict()
    for pod_name in prowjob_df[attr]:
        if not pod_name:
            continue
        prefixes = list(trie.iter_prefix_items(pod_name))
        name_mapping[pod_name] = pod_name  # Default to most specific name if the pod name exists in only one job
        for prefix in reversed(prefixes):
            # If the prefix was in more than one prowjob, prefer it.
            if len(prefix[1]) > 1:
                if pod_name == prefix[0]:
                    name_mapping[pod_name] = prefix[0]
                else:
                    name_mapping[pod_name] = prefix[0] + '-...'
                break
    return name_mapping


if __name__ == '__main__':

    start_date = "2024-11-01"
    span = "INTERVAL 1 DAY"
    search_window_intervals = {
        "before": "INTERVAL 2 SECOND",
        "after": "INTERVAL 5 SECOND"
    }

    cache_file = pathlib.Path(f'cache-{start_date}.parquet')
    if cache_file.exists():
        print('USING CACHE!')
        df = pandas.read_parquet(str(cache_file))
    else:
        bq_client = bigquery.Client(project='openshift-gce-devel')

        # disruption_events includes all rows in which there is a disruption event in the specified date range.
        # numbered_disruptions,
        relevant_events = f"""
WITH disruption_events AS (
  SELECT 
      d.prowjob_name as prowjob_name,
      d.prowjob_build_id as prowjob_build_id,
    
      d.from_time as d_from_time,
      d.to_time as d_to_time,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.message"), r'reason/([a-zA-Z0-9.-]+)') AS d_reason,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.message"), r'cause/([a-zA-Z0-9.-]+)') AS d_cause, 
      IFNULL(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'ns/([a-zA-Z0-9.-]+)'), REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'namespace/([a-zA-Z0-9.-]+)')) AS d_ns,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'disruption/([a-zA-Z0-9.-]+)') AS d_disruption,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'route/([a-zA-Z0-9.-]+)') AS d_route,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'connection/([a-zA-Z0-9.-]+)') AS d_connection,
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.locator"), r'backend-disruption-name/([a-zA-Z0-9.-]+)') AS d_backend_disruption_name,
      JSON_EXTRACT_SCALAR(d.payload, "$.message") AS d_message,
      JSON_EXTRACT_SCALAR(d.payload, "$.locator") AS d_locator,
      d.payload as d_payload,
  FROM 
    `openshift-gce-devel.ci_analysis_us.job_intervals` d
  WHERE 
    REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(d.payload, "$.message"), r'reason/([a-zA-Z0-9.-]+)') LIKE 'DisruptionBegan'
    AND d.from_time BETWEEN DATETIME("{start_date}") AND DATETIME_ADD("{start_date}", {span})
),
numbered_disruption_events AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY prowjob_build_id ORDER BY d_from_time ASC) AS row_num
  FROM 
    disruption_events
),
d AS (
    SELECT 
      *
    FROM 
      numbered_disruption_events
    WHERE 
      row_num = 1
)        
SELECT 
  d.*,

  e.from_time as e_from_time,
  e.to_time as e_to_time,
  IFNULL(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'ns/([a-zA-Z0-9.-]+)'), REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'namespace/([a-zA-Z0-9.-]+)')) AS e_ns,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'(node)/[a-zA-Z0-9.-]+') AS e_node,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'pod/([a-zA-Z0-9.-]+)') AS e_pod,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'container/([a-zA-Z0-9.-]+)') AS e_container,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'uid/([a-zA-Z0-9.-]+)') AS e_uid,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.locator"), r'row/([a-zA-Z0-9.-]+)') AS e_row,

  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.message"), r'reason/([a-zA-Z0-9.-]+)') AS e_reason,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.message"), r'cause/([a-zA-Z0-9.-]+)') AS e_cause, 
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.message"), r'constructed/([a-zA-Z0-9.-]+)') AS e_constructed,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.message"), r'count/([a-zA-Z0-9.-]+)') AS e_count,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(e.payload, "$.message"), r'(roles?/[a-zA-Z0-9.-]*)') AS e_roles,
  
  JSON_EXTRACT_SCALAR(e.payload, "$.locator") AS e_locator,
  JSON_EXTRACT_SCALAR(e.payload, "$.message") AS e_message,

FROM `openshift-gce-devel.ci_analysis_us.job_intervals` e
JOIN d
ON e.prowjob_build_id = d.prowjob_build_id

WHERE 
  JSON_EXTRACT_SCALAR(e.payload, "$.message") NOT LIKE "%reason/DisruptionBegan %"
  AND e.from_time BETWEEN DATETIME("{start_date}") AND DATETIME_ADD("{start_date}", {span}) 
  AND d.d_from_time BETWEEN DATETIME_SUB(e.from_time, {search_window_intervals["after"]}) AND DATETIME_ADD(e.from_time, {search_window_intervals['after']})
ORDER BY e.prowjob_build_id, e.from_time ASC
"""
        df = bq_client.query(relevant_events).to_dataframe(create_bqstorage_client=True, progress_bar_type='tqdm')
        df.to_parquet(str(cache_file))

    regex = r'-[0-9a-fA-F]{8,10}(-.*|$)'
    df['e_pod'] = df['e_pod'].str.replace(regex, "-xxxxxxxxxx", regex=True)

    unique_pods = df[['prowjob_build_id', 'e_pod']].drop_duplicates()
    pod_name_mapping = uniquest_prefix_in_prowjobs(unique_pods, 'e_pod')
    unique_namespaces = df[['prowjob_build_id', 'e_ns']].drop_duplicates()
    namespace_name_mapping = uniquest_prefix_in_prowjobs(unique_namespaces, 'e_ns')

    df['e_pod'] = df['e_pod'].map(pod_name_mapping)
    df['e_ns'] = df['e_ns'].map(namespace_name_mapping)
    event_fields = [
        'e_node',
        'e_ns',
        'e_pod',
        'e_container',
        'e_reason',
        'e_cause',
        'e_roles',
        'e_row',
    ]
    df = df.dropna(subset=event_fields, how='all')
    df['event'] = df[event_fields].fillna('_').astype(str).apply(lambda row: ':'.join(row), axis=1)

    df['event_hash'] = pandas.util.hash_pandas_object(df['event'], index=False)
    full_def = df
    # Remove sequential rows with the same prowjob_build_id and event_hash.
    df = df[df[['prowjob_build_id', 'event_hash']].ne(df[['prowjob_build_id', 'event_hash']].shift()).any(axis=1)]

    # Find all of unique event_hashes.
    unique_hashes = df[['event_hash', 'event', 'e_reason']].drop_duplicates()
    # Build a dictionary of event_hash=>event string to help lookup event information once the hashes are known
    event_hash_to_event: Dict[str, str] = unique_hashes.set_index('event_hash')['event'].to_dict()

    # given a dataframe with columns prowjob_build_id, event, from_time,
    # create a list of lists where each element
    # in the list is all events from a given prowjob_build_id, ordered
    # by from_time.
    grouped_by_prowjob = df.sort_values(by='e_from_time').groupby('prowjob_build_id')
    result = (
        grouped_by_prowjob['event_hash']
        .apply(list)
        .to_dict()
    )

    # Group by prowjob_build_id and get the first row for each group
    first_rows = grouped_by_prowjob.first()
    # Convert the result to a dictionary (prowjob_build_id => first_row)
    first_rows_dict = first_rows.to_dict(orient='index')

    # Create a PrefixSpan instance and mine frequent sequences
    print(f'Building PrefixSpan')
    ps = PrefixSpan(list(result.values()))
    ps.minlen = 2
    ps.maxlen = 5

    # Find frequent patterns with a minimum support of 0.5 (50%)
    print(f'Building patterns')
    patterns = ps.topk(5, closed=True)

    # Print the frequent patterns
    print(f'Printing patterns')
    for count, pattern in patterns:
        print(f'Count: {count}')
        print('Events:')
        for idx, entry in enumerate(pattern):
            print(f' {idx+1}. {event_hash_to_event[entry]}')

        print("Matches prowjobs:")
        for prowjob_build_id, sequence in result.items():
            if is_subsequence(pattern, sequence):
                print(f" - {prowjob_build_id} @ {first_rows_dict[prowjob_build_id]['d_from_time']}")
        print()

    # Select relevant events around the time of the disruption
    handy_query = """
WITH variables AS (
    SELECT 
        DATETIME("2024-11-01 02:24:26") AS disruption_start,
        "1852142061688459264" AS prowjob_build_id
)
SELECT * FROM `openshift-gce-devel.ci_analysis_us.job_intervals` intervals JOIN variables ON intervals.prowjob_build_id=variables.prowjob_build_id WHERE 
from_time BETWEEN DATETIME_SUB(variables.disruption_start, {search_window_intervals['before']}) AND DATETIME_ADD(variables.disruption_start, {search_window_intervals['after']})    
    """