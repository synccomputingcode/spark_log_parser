#!/bin/bash

HOME="$( cd -- "$(dirname "$0")" &>/dev/null; pwd -P )"

function print_usage() {
  cat <<-EOD
	Usage: $0 -l <event log path> [-r <results directory>]
	Options:
	  -l <event log path>     path to event log file
	  -r <results directory>  directory in which to save parsed logs
	  -h                      show this helpful message and exit
	EOD
}

results_dir="$HOME/results"
args=()

while getopts l:r:h name; do
  case $name in
    l)
      event_log="$OPTARG"
      args=("${args[@]}" -l "$OPTARG")
      ;;
    r)
      results_dir="$OPTARG"
      ;;
    h)
      print_usage
      exit
      ;;
    *)
      >&2 print_usage
      exit 1
      ;;
  esac
done

if [[ -z $event_log ]]; then
  >&2 echo "event log file path is required"
  >&2 print_usage
  exit 1
fi

args=("${args[@]}" -r "$results_dir")

exec python3 -m spark_log_parser "${args[@]}"
