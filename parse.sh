#!/bin/bash

HOME="$( cd -- "$(dirname "$0")" &>/dev/null; pwd -P )"

function print_usage() {
  cat <<-EOD
	Usage:
	  $0 -l <event log path> [-r <results directory>]
	  $0 --version
	Options:
	  -l <event log path>     path to event log file
	  -r <results directory>  directory in which to save parsed logs
	  --version               print version information and exit
	  -h                      show this helpful message and exit
	EOD
}

if [[ $1 == "--version" ]]; then
  exec spark_log_parser --version
fi

results_dir="$HOME/results"

while getopts l:r:h name; do
  case $name in
    l)
      event_log="$OPTARG"
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

exec spark_log_parser -l "$event_log" -r "$results_dir"
