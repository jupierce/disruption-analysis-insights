#!/usr/bin/env python3
import os
import hashlib

from typing import Dict
import pathlib
import json

import pytrie

import pprint
import pandas
from prefixspan import PrefixSpan
from google.cloud import bigquery, bigquery_storage

from jinja2 import Template
import webbrowser


# Function to check if a pattern is a subsequence of a sequence
def is_subsequence(pattern, sequence):
    it = iter(sequence)  # When it is an iterator, all respects order of occurrence
    return all(item in it for item in pattern)


def anonymize_unique_strings(prowjob_df: pandas.DataFrame, attr: str) -> Dict[str, str]:
    # Count distinct prowjob_build_id occurrences for each message
    message_counts = df.groupby(attr)["prowjob_build_id"].nunique()
    message_dict = {
        message: "" if count == 1 else message
        for message, count in message_counts.items()
    }
    return message_dict


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

    jobs_table_id = 'openshift-gce-devel.ci_analysis_us.jobs'
    intervals_table_id = 'openshift-ci-data-analysis.ci_data_autodl.e2e_intervals'
    start_date = "2024-11-06"
    span = "INTERVAL 1 DAY"

    search_window_intervals = {
        "before": "INTERVAL 1 SECOND",  # Include events that the disruption was slightly before
        "after": "INTERVAL 15 SECOND"  # Include events that the disruption was slightly after
    }

    job_name_matches = (
        "4.18",
        "gcp"
    )

    job_name_condition = " AND ".join([f'jobs.prowjob_job_name LIKE "%{entry}%"' for entry in job_name_matches])
    d_interval_field = "IFNULL(d.interval, d.interval_json)"
    e_interval_field = "IFNULL(e.interval, e.interval_json)"

    partials = []  # rendered partial templates for each disruption
    cache_dir = pathlib.Path('cache')
    cache_dir.mkdir(exist_ok=True)
    for target_disruption_index, target_disruption_match in enumerate(['service-load-balancer-%', 'ingress-to-%', 'cache-kube-api-%', 'cache-%', 'kube-api-%', 'openshift-api-%', 'oauth-api-%', 'host-to-%', 'pod-to-%']):

        cache_file = cache_dir.joinpath(f'cache-{start_date}.{target_disruption_match}')
        if cache_file.exists():
            print('USING CACHE!')
            df = pandas.read_parquet(str(cache_file))
        else:
            bq_client = bigquery.Client(project='openshift-gce-devel')

            # first_loki_pod_created:
            #   for each prowjob, find the to_time for successfully
            # disruption_events includes all rows in which there is a disruption event in the specified date range.
            # numbered_disruptions,
            relevant_events = f"""
    WITH disruption_events AS (
      SELECT 
        jobs.prowjob_job_name as prowjob_job_name,
        jobs.prowjob_build_id as prowjob_build_id,
        jobs.prowjob_url as prowjob_url,
        
          d.from_time as d_from_time,
          d.to_time as d_to_time,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.message.reason") AS d_reason,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.message.cause") AS d_cause, 
          IFNULL(JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.namespace"), JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.ns")) AS d_ns,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.disruption") AS d_disruption,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.route") AS d_route,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.connection") AS d_connection,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.backend-disruption-name") AS d_backend_disruption_name,
          JSON_EXTRACT_SCALAR({d_interval_field}, "$.message.humanMessage") AS d_message,
          {d_interval_field} as d_payload,
      FROM 
        `{intervals_table_id}` d
      JOIN
        `{jobs_table_id}` jobs
      ON 
        jobs.prowjob_build_id = d.JobRunName
      WHERE 
        JSON_EXTRACT_SCALAR({d_interval_field}, "$.message.reason") LIKE 'DisruptionBegan'
        AND `source` != "e2e-events-observer.json"
        AND JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.backend-disruption-name") NOT LIKE "%-liveness-%"
        AND JSON_EXTRACT_SCALAR({d_interval_field}, "$.display")  = "true"
        AND d.from_time BETWEEN TIMESTAMP("{start_date}") AND TIMESTAMP_ADD("{start_date}", {span})
        AND jobs.prowjob_start BETWEEN DATETIME("{start_date}") AND DATETIME_ADD("{start_date}", {span})
        AND jobs.prowjob_job_name NOT LIKE "%single-node%" 
        AND {job_name_condition}
        AND JSON_EXTRACT_SCALAR({d_interval_field}, "$.locator.keys.backend-disruption-name") LIKE "{target_disruption_match}"
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
      IF(TIMESTAMP_DIFF(d_from_time, e.from_time, SECOND) = 0, "at_d", IF(TIMESTAMP_DIFF(d_from_time, e.from_time, SECOND) <= 0, "after_d", "before_d")) as e_diff,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.source") AS e_source,
      IFNULL(JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.namespace"), JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.ns")) AS e_ns,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.node") AS e_node,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.pod") AS e_pod,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.container") AS e_container,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.uid") AS e_uid,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.row") AS e_row,
    
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.reason") AS e_reason,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.cause") AS e_cause, 
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.annotations.constructed") AS e_constructed,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.count") AS e_count,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.roles") AS e_roles,
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.humanMessage") AS e_message,
      
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator") AS e_locator,
      TO_JSON_STRING({e_interval_field}) AS e_payload,
    
    FROM `{intervals_table_id}` e
    JOIN d
    ON e.JobRunName = d.prowjob_build_id
    
    WHERE 
      JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.reason") NOT LIKE "DisruptionEnded"
      AND
      (
        JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.reason") NOT LIKE "DisruptionBegan"
        OR JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.backend-disruption-name") LIKE "%-liveness-%"
      )
      AND NOT (
        # Service load balancer disruption usually indicate a master node going down.
        # The kube-apiserver is heavily instrumented and is reporting node going down.
        # Just filter these. Same for CSI drivers and openshift-iamge-registry. 
        d.d_backend_disruption_name = "service-load-balancer-with-pdb-reused-connections" AND 
        (
            (
                JSON_EXTRACT_SCALAR({e_interval_field}, "$.source") = "KubeEvent" AND
                (
                    (
                        # Namespaces likely to complain when node is down.
                        IFNULL(JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.namespace"), JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.ns")) = "openshift-kube-apiserver"
                        OR
                        IFNULL(JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.namespace"), JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.ns")) = "openshift-image-registry"
                        OR 
                        IFNULL(JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.namespace"), JSON_EXTRACT_SCALAR({e_interval_field}, "$.locator.keys.ns")) = "openshift-cluster-csi-drivers"
                    )
                    OR
                    (
                        # Scheduling is going to fail while node is down.
                        JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.reason") = "FailedScheduling"
                    )
                )
            ) 
            OR 
            (
                # etcd is also generating noise we can filter at this time.
                JSON_EXTRACT_SCALAR({e_interval_field}, "$.source") = "EtcdLog"
            )
            OR 
            (
                # Alerts for KubeScheduling 
                JSON_EXTRACT_SCALAR({e_interval_field}, "$.source") = "Alert"
                AND
                JSON_EXTRACT_SCALAR({e_interval_field}, "$.message.humanMessage") LIKE "%KubeDaemon%"
            )
        )
      )
      AND e.from_time BETWEEN TIMESTAMP("{start_date}") AND TIMESTAMP_ADD("{start_date}", {span}) 
      AND d.d_from_time BETWEEN TIMESTAMP_SUB(e.from_time, {search_window_intervals["before"]}) AND TIMESTAMP_ADD(e.from_time, {search_window_intervals['after']})
      
    ORDER BY e.JobRunName, e.from_time ASC
    """
            print(relevant_events)
            query_results = bq_client.query(relevant_events)
            if query_results.result().total_rows > 0:
                df = query_results = query_results.to_dataframe(create_bqstorage_client=True, progress_bar_type='tqdm')
            else:
                df = pandas.DataFrame()
            df.to_parquet(str(cache_file))

        if len(df.index) == 0:
            template = Template("""
                <br>
                <h2> Patterns for disruption LIKE {{ target_disruption_match }}</h2>
                <div style="padding-left:20px;">
                    No matching disruptions found in search.
                </div>
                <br>
            """)
            partials.append(template.render(target_disruption_match=target_disruption_match))
            continue

        # Pods occasionally have random hex sequences. Anonymize them.
        regex = r'-[0-9a-fA-F]{8,10}(-.*|$)'
        df['e_pod'] = df['e_pod'].str.replace(regex, "-xxxxxxxxxx", regex=True)

        # Nodes are based on IP usually. Anonymize them. Leave as NA if not set.
        regex = r'[a-zA-Z0-9.-]+'
        df['e_node'] = df['e_node'].fillna('no')
        df['e_node'] = df['e_node'].str.replace(regex, "yes", regex=True)

        unique_pods = df[['prowjob_build_id', 'e_pod']].drop_duplicates()
        pod_name_mapping = uniquest_prefix_in_prowjobs(unique_pods, 'e_pod')
        unique_namespaces = df[['prowjob_build_id', 'e_ns']].drop_duplicates()
        namespace_name_mapping = uniquest_prefix_in_prowjobs(unique_namespaces, 'e_ns')

        df['e_pod'] = df['e_pod'].map(pod_name_mapping)
        df['e_ns'] = df['e_ns'].map(namespace_name_mapping)

        regex = r'[0-9a-fA-F]{8,30}'  # Anonymize any long hex string
        df['e_message'] = df['e_message'].str.replace(regex, "xxxxxxxxxx", regex=True)
        regex = r'[0-9]{1,30}'  # Anonymize any simple numbers
        df['e_message'] = df['e_message'].str.replace(regex, "###", regex=True)
        df['e_message'] = df['e_message'].map(anonymize_unique_strings(df, 'e_message'))

        event_fields = [
            'e_diff',
            'e_source',
            'e_node',
            'e_ns',
            'e_pod',
            'e_container',
            'e_reason',
            'e_cause',
            'e_roles',
            'e_row',
            'e_message',
        ]
        df = df.dropna(subset=event_fields, how='all')  # Drop rows with no interesting information
        df['event'] = df[event_fields].fillna('_').astype(str).apply(
            lambda row: ':'.join(f"{col}={val}" for col, val in zip(row.index, row)),
            axis=1
        )

        df['event_hash'] = pandas.util.hash_pandas_object(df['event'], index=False)
        full_def = df

        # Sorting on fields after from_time ensures that if a sequence of messages occurs in the same
        # second, they are sorted, and thus can be consistent between more prowjobs (leading to
        # additional occurrences).
        df = df.sort_values(by=['prowjob_build_id', 'e_from_time', 'e_from_time', 'e_source', 'e_ns', 'e_reason', 'e_message'])

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
        grouped_by_prowjob = df.groupby('prowjob_build_id', sort=False)

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
        ps.minlen = 1
        ps.maxlen = 10

        # Find frequent patterns with a minimum support of 0.5 (50%)
        print(f'Building patterns')
        patterns = ps.topk(25, closed=True)

        entries = []

        # Get a reduced dataframe only including rows for
        # prowjobs in the report. This is all to speed up the search
        # for qualifying events.
        df_for_reported_prowjobs = df[
            df["prowjob_build_id"].isin(list(result.keys()))
        ]
        # Group by prowjob_build_id and event_hash, and take the first occurrence for each
        first_event_hash_occurrences = df_for_reported_prowjobs.sort_values("e_from_time").groupby(
            ["prowjob_build_id", "event_hash"], as_index=False
        ).first()
        # Set the index to prowjob_build_id + event_hash for quick lookups
        first_event_hash_occurrences.set_index(["prowjob_build_id", "event_hash"], inplace=True)

        # Print the frequent patterns
        print(f'Printing patterns')
        for count, pattern in patterns:
            print(f'Count: {count}')
            print('Events:')
            event_sequence = []
            for event_hash in pattern:
                event_sequence.append(event_hash_to_event[event_hash])

            df_for_reported_prowjob_events = df_for_reported_prowjobs[
                df_for_reported_prowjobs["event_hash"].isin(pattern)
            ]

            matching_prowjobs = []
            for prowjob_build_id, sequence in result.items():
                df_for_prowjob_events = df_for_reported_prowjob_events[
                    df_for_reported_prowjob_events["prowjob_build_id"] == prowjob_build_id
                ]
                if is_subsequence(pattern, sequence):
                    first_row = first_rows_dict[prowjob_build_id]
                    example_rows = []

                    # Select relevant events around the time of the disruption
                    handy_query = f"""
    WITH variables AS (
        SELECT 
            TIMESTAMP("{str(first_row['d_from_time'])}") AS disruption_start,
            "{prowjob_build_id}" AS prowjob_build_id
    )
    SELECT * 
    FROM 
        `openshift-ci-data-analysis.ci_data_autodl.e2e_intervals` intervals JOIN variables 
    ON 
        intervals.JobRunName=variables.prowjob_build_id 
    WHERE 
        from_time BETWEEN TIMESTAMP_SUB(variables.disruption_start, {search_window_intervals['after']}) 
        AND TIMESTAMP_ADD(variables.disruption_start, {search_window_intervals['before']})   
        AND `source` != "e2e-events-observer.json"
        # AND TO_JSON_STRING(IFNULL(`interval`, `interval_json`)) LIKE "%something in interval%" 
    ORDER BY from_time
    """

                    prowjob_info = {
                        'id': f"{prowjob_build_id}",
                        'query': handy_query,
                        'disruption_time': str(first_row['d_from_time']),
                        'name': first_row['prowjob_job_name'],
                        'url': first_row['prowjob_url'],
                        'disruption': first_row['d_disruption'],
                        'namespace': first_row['d_ns'],
                        'backend_disruption_name': first_row['d_backend_disruption_name'],
                        'connection': first_row['d_connection'],
                        'route': first_row['d_route'],
                        'message': first_row['d_message'],
                        "example_rows": example_rows,
                    }

                    # TODO: This seems slow
                    for event_hash in pattern:
                        example_row = first_event_hash_occurrences.loc[(prowjob_build_id, event_hash)]
                        example_rows.append(json.loads(example_row['e_payload']))
                    matching_prowjobs.append(prowjob_info)

            if count > 1:
                entries.append({
                    'count': count,
                    'sequence': event_sequence,
                    'prowjobs': matching_prowjobs,
                })

        template = Template("""
            <br>
            <h2><span class="toggle-btn" onclick="toggleDetails('disruption-{{ target_disruption_index }}')">
                                &#x25BC; 
                            </span> Patterns for disruption LIKE {{ target_disruption_match }}</h2>
            <div style="padding-left:20px;" class="prowjob-details" id="details-disruption-{{ target_disruption_index }}">
            
            <br>
            {% for entry in entries %}
                <section>
                
                    <h2>Pattern {{ loop.index }}</h2>
                    <h3>Occurrences: {{ entry.count }}</h3>
                    
                    
                    <ol>
                    {% for seq in entry.sequence %}
                        <li>{{ seq | replace(':', ' ') }}</li>
                    {% endfor %}
                    </ol>
            
                    <h3><span class="toggle-btn" onclick="toggleDetails('{{ target_disruption_index }}-{{ loop.index }}')">
                                &#x25BC; 
                            </span>Prowjobs</h3>
                    <div class="prowjob-details" id="details-{{ target_disruption_index }}-{{ loop.index }}">
                    <ol>
                    {% set outer_loop = loop %}
                    {% for prowjob in entry.prowjobs %}
                        <li>
                        <div>
                            <span class="toggle-btn" onclick="toggleDetails('{{ target_disruption_index }}-{{ outer_loop.index }}-{{ loop.index }}-{{ prowjob.id }}')">
                                &#x25BC; 
                            </span>
                            <span>{{ prowjob.name }} <a href="{{ prowjob.url }}" target="_blank">{{ prowjob.id }}</a> {{ prowjob.backend_disruption_name }} @ {{ prowjob.disruption_time }}</span>
                            <div class="prowjob-details" id="details-{{ target_disruption_index }}-{{ outer_loop.index }}-{{ loop.index }}-{{ prowjob.id }}">
                                <strong>Disruption:</strong> {{ prowjob.disruption }}<br>
                                <ul>
                                    <li><strong>Time:</strong> {{ prowjob.disruption_time }}
                                    <li><strong>Backend:</strong> {{ prowjob.backend_disruption_name }}
                                    <li><strong>Namespace:</strong> {{ prowjob.namespace }}
                                    <li><strong>Connection:</strong> {{ prowjob.connection }}
                                    <li><strong>Route:</strong> {{ prowjob.route }}
                                    <li><strong>Message:</strong> {{ prowjob.message }}
                                </ul>
                                <br>
                                <strong>Events in search window:</strong> 
                                <pre>{{ prowjob.query }}</pre>
                                <br>
                                <strong>Example Qualifying Events</strong>
                                <ol>
                                    {% for example in prowjob.example_rows %}
                                        <li><pre> {{ example | tojson(indent=4) }} </pre> </li>
                                    {% endfor %}
                                </ol>
                            </div>
                        </div>
                        </li>
                    {% endfor %}
                    </ol>
                    </div>
            
                </section>
            {% endfor %}
            </div>
        """)
        partial_content = template.render(
                                       entries=entries,
                                       target_disruption_match=target_disruption_match,
                                       target_disruption_index=target_disruption_index,
                                       )
        partials.append(partial_content)

    master_template = Template("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ title }}</title>
        <style>
            .prowjob-details {
                display: none;
                margin-top: 10px;
                background-color: #f9f9f9;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 4px;
            }
            .toggle-btn {
                cursor: pointer;
                color: #007bff;
            }
        </style>        
    </head>
    <body>
            <script>
                function toggleDetails(prowjobId) {
                    var details = document.getElementById("details-" + prowjobId);
                    if (details.style.display === "none" || details.style.display === "") {
                        details.style.display = "block";
                    } else {
                        details.style.display = "none";
                    }
                }
            </script>

        <h1>Sequence Mining / Frequent Sequence Before First Disruption</h1>
        <h2>{{ heading }}</h2>
        <h2>{{ constraints }}</h2>

        <br>
        {{ partials_combined | safe }}
    </body>
    </html>
    """)

    html_content = master_template.render(title='Interval Insights',
                                          heading=f'{start_date} for span {span}',
                                          constraints=f'Events up to {search_window_intervals["after"]} before disruption and {search_window_intervals["before"]} after disruption',
                                          partials_combined='\n'.join(partials),
                                          )

    file_path = "generated_page.html"
    with open(file_path, "w") as file:
        file.write(html_content)

    webbrowser.open(f"file://{os.path.abspath(file_path)}")

    print(handy_query)