#!/usr/bin/env python

import argparse
import logging

# TODO: Add a requirements.txt
import pandas as pd

global LOGGER
SEP = "\t"


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description="Aggregate and summarize Patelco account information from CSV files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    arg_parser.add_argument("-d", "--debug", action="store_true", help="print DEBUG logs (default log level is INFO)")
    arg_parser.add_argument("-f", "--field", default="Description", help="The name of the field to group by")
    arg_parser.add_argument("-t", "--type", default="Debit", help="Comma-separated list of transaction types, e.g. Credit,Debit")
    arg_parser.add_argument("paths", nargs="+", help="One or more paths to input file(s).")
    return arg_parser.parse_args()


def main(args):
    transaction_data = read_files(args.paths)
    trans_by_type = {}
    for trans_type in args.type.split(","):
        trans_by_type[trans_type] = aggregate_transactions_by_type(transaction_data, args.field, trans_type)

    for trans_type in trans_by_type:
        LOGGER.debug(f"{trans_type}\n{trans_by_type[trans_type]}")


def read_files(filelist):
    all_dfs = []
    for path in filelist:
        all_dfs.append(pd.read_csv(path))
        LOGGER.debug(f"Headers in {path}: {SEP.join([str(h) for h in all_dfs[-1].columns])}")

    transaction_data = pd.concat(all_dfs)
    LOGGER.debug(f"All headers: {SEP.join([str(h) for h in transaction_data.columns])}")
    for col_name in transaction_data.columns:
        if col_name.lower().endswith("date"):
            transaction_data[col_name] = pd.to_datetime(transaction_data[col_name])

    LOGGER.debug(f"transaction_data.dtypes:\n{transaction_data.dtypes}")
    return transaction_data


def aggregate_transactions_by_type(transdata, field_name, transtype):
    result = []
    try:
        trans_by_type = transdata.loc[transdata["Transaction Type"] == transtype]
        LOGGER.info(f"Loaded {transtype}s: {len(trans_by_type)}")
    except KeyError:
        LOGGER.error(f"Field not found in input: 'Transaction Type'")
        return None

    trans_by_type.set_index(field_name)
    try:
        trans_count_by_field = trans_by_type.groupby(field_name)[field_name].agg("count")
        LOGGER.info(f"Loaded unique {transtype}s by {field_name}: {len(trans_count_by_field)}")
        LOGGER.debug(f"{transtype}s by {field_name} summary:\n{trans_count_by_field}")
        result.append(trans_count_by_field)
    except KeyError as exc:
        LOGGER.error(f"Field not found in input: {exc}")
        return None

    try:
        trans_amount_sum = trans_by_type.groupby(field_name)["Amount"].agg(sum)
        LOGGER.debug(f"{transtype} Amount Sum by {field_name} summary:\n{trans_amount_sum}")
        result.append(trans_amount_sum)
    except KeyError as exc:
        LOGGER.warning(f"Field not found in input: {exc}")

    try:
        trans_amount_mean = trans_by_type.groupby(field_name)["Amount"].agg("mean")
        LOGGER.debug(f"{transtype} Amount Mean by {field_name} summary:\n{trans_amount_mean}")
        result.append(trans_amount_mean)
    except KeyError as exc:
        LOGGER.warning(f"Field not found in input: {exc}")

    try:
        trans_date_min = trans_by_type.groupby(field_name)["Effective Date"].agg(min)
        LOGGER.debug(f"{transtype} Earliest Date by {field_name} summary:\n{trans_date_min}")
        result.append(trans_date_min)
    except KeyError as exc:
        LOGGER.warning(f"Field not found in input: {exc}")

    try:
        trans_date_max = trans_by_type.groupby(field_name)["Effective Date"].agg(max)
        LOGGER.debug(f"{transtype} Latest Date by {field_name} summary:\n{trans_date_max}")
        result.append(trans_date_max)
    except KeyError as exc:
        LOGGER.warning(f"Field not found in input: {exc}")

    retval = pd.DataFrame().set_index(field_name)
    return retval.join(other=result, on=field_name)


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s %(message)s",
        level=logging.DEBUG if args.debug else logging.INFO
    )
    LOGGER = logging.getLogger(__name__)
    main(args)
