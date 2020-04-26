from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from itertools import cycle, groupby, islice, product, repeat
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import sys
import uuid

from statistics import mean, median, stdev
from statsmodels.distributions.empirical_distribution import ECDF

# Default file prefixes
latency_plot_file_prefix = "latency_plot_"
throughput_plot_file_prefix = "throughput_plot_"
latency_file_prefix = "latencies"
throughput_file_prefix = "throughputs"
dump_file_prefix = "dump_"

parser = argparse.ArgumentParser()
# Plot-related parameteres
parser.add_argument("--plot-type", default="latency",
                    choices = ["api_overhead", "latency",
                               "throughput", "latency_vs_throughput"],
                    help="the number of batches prefetched from plasma")
parser.add_argument("--cdf", default=False,
                    action = 'store_true',
                    help="whether to generate latency CDFs instead of lines")
parser.add_argument("--logscale", default=False,
                    action = 'store_true',
                    help="whether to generate a logscale plot")
parser.add_argument("--plot-repo", default="./",
                    help="the folder to store plots")
parser.add_argument("--plot-file", default=latency_plot_file_prefix,
                    help="the plot file prefix")
# File-related parameteres
parser.add_argument("--file-repo", default="./",
                    help="the folder containing the log files")
parser.add_argument("--latency-file", default=latency_file_prefix,
                    help="the file containing per-record latencies")
parser.add_argument("--throughput-file", default=throughput_file_prefix,
                    help="the file containing actors throughput")
parser.add_argument("--files", required=True,
                    help="the file containing the paths to log files")
parser.add_argument("--labels", required=True,
                    help="the file containing the labels of the plot")


# 'agg' backend is used to create plot as a .png file
mpl.use('agg')

# Colors
reds = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                             cmap="Reds")
greens = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                               cmap="Greens")
blues = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Blues")
oranges = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Oranges")
greys = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Greys")

# Line color generator
colors = cycle([blues, greys, reds, greens, oranges])

# Linestyle generator
linestyles = cycle(['solid', 'dashed', 'dotted', 'dashdot'])

# Generates plot UUIDs
def _generate_uuid():
    return uuid.uuid4()

# Collects per-record latencies from log files
def collect_latencies(latency_filepaths):
    all_latencies = []  # One list of measured latencies per file
    for filepath in latency_filepaths:
        print("Reading file: {}".format(filepath))
        latencies = []
        try:  # Read latencies
            with open(filepath,"r") as lf:
                for latency in lf:
                    latencies.append(float(latency))
                all_latencies.append(latencies)
        except FileNotFoundError:
            sys.exit("Could not find file '{}'".format(filepath))
    return all_latencies

# Collects actor input/output rates from all necessary files
def collect_rates(throughput_filepaths):
    all_rates = []  # One list of measured rates per file
    for filepath in throughput_filepaths:
        print("Reading file: {}".format(filepath))
        rates = []
        raw_rates = {}
        try:
            with open(filepath,"r") as lf:
                for rate_log in lf:
                    # actor_id | in_rate | out_rate
                    log = rate_log.strip().split("|")
                    log = [element.strip() for element in log]
                    assert(len(log) >= 2)
                    if len(log) < 3:  # In case of plain-queue experiments
                        log.append(log[1])  # set in_rate = out_rate
                    entry = raw_rates.setdefault(log[0],([],[]))
                    entry[0].append(float(log[1]))
                    entry[1].append(float(log[2]))
                # Compute mean rates and variance for each actor
                for actor_id, (in_rate, out_rate) in raw_rates.items():
                    if len(in_rate) > 1:
                        in_mean = median(in_rate)
                        in_stdev = 0.0 #stdev(in_rate,in_mean)
                    else:
                        in_mean = float(in_rate[0])
                        in_stdev = 0.0
                    if len(out_rate) > 1:
                        out_mean = median(out_rate)
                        out_stdev = 0.0 #stdev(out_rate,out_mean)
                    else:
                        out_mean = float(out_rate[0])
                        out_stdev = 0.0
                    rates.append((actor_id, in_mean, in_stdev,
                                  out_mean, out_stdev))
            all_rates.append(rates)
        except FileNotFoundError:
            sys.exit("Could not find file '{}'".format(filepath))
    return all_rates

# Generates a line plot
def generate_line_plot(x_data, y_data, x_label, y_label, line_labels,
                       plot_repo, plot_file_name,
                       cdf=False, point_labels=None, logscale=False):
    # Create a figure instance
    line_plot = plt.figure(1, figsize=(9, 6))
    # Create an axes instance
    ax = line_plot.add_subplot(111)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    i = 0
    if cdf:  # Generate CDF
        for data_x in x_data:
            label = line_labels[i]
            cdf = ECDF(data_x)
            ax.plot(cdf.x, cdf.y, label=label,
                    linestyle=next(linestyles), alpha=0.5,
                    linewidth=1, c=next(colors).to_rgba(1))
            i += 1
    else:  # Generate line plot
        for data_x in x_data:
            label = line_labels[i]
            data_y = y_data[i]
            ax.plot(data_x, data_y, label=label,
                    linestyle=next(linestyles), alpha=0.5,
                    linewidth=1, c=next(colors).to_rgba(1))
            i += 1
    ax.legend(fontsize=8)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    if logscale:
        ax.set_yscale('log')
    # Set font sizes
    for item in ([ax.title, ax.xaxis.label,
        ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(16)
    if point_labels is not None:  # Annotate data points
        assert not cdf  # Cannot annotate CDF points
        for data_x, data_y in zip(x_data, y_data):
            for x,y,label in zip(data_x,data_y,point_labels):
                ax.annotate(label,xy=(x,y))
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    line_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    line_plot.clf()

# Generates a boxplot
def generate_box_plot(data, x_label, y_label,
                      labels, plot_repo, plot_file_name, logscale=False):
    # Create a figure instance
    box_plot = plt.figure(1, figsize=(9, 6))
    # Create an axes instance
    ax = box_plot.add_subplot(111)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.boxplot(data)
    if logscale:
        ax.set_yscale('log')
    ax.set_xticklabels(labels)
    # Set font sizes
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label]):
        item.set_fontsize(14)
    for item in (ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(10)
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    box_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    box_plot.clf()

# Generates a barchart
def generate_barchart_plot(data, x_label, y_label, bar_metadata, exp_labels,
                           plot_repo, plot_file_name, logscale=False):
    # TODO (john): Add option to use actor ids at the bottom of each bar
    num_exps = len(data)        # Total number of histograms in the plot
    ind = np.arange(num_exps)   # The x-axis locations of the histograms
    bar_width = 1               # The width of each bar in a histogram
    bar_plot, ax = plt.subplots()
    pos = 0
    step = bar_width
    for histogram, histogram_metadata in zip(data, bar_metadata):
        for value, value_metadata in zip(histogram, histogram_metadata):
            label = value_metadata[0] if value_metadata[3] else ""
            color = value_metadata[1] if value_metadata[1] else next(
                                                            colors).to_rgba(1)
            bar = ax.bar(pos + bar_width,
                           value,
                           label=label,
                           color=color,
                           alpha=0.5,
                           width=bar_width,
                           yerr=0)
            if value_metadata[2]:  # Hatch bar
                bar[0].set_hatch("/")
            pos += step
        pos += step  # Separate histograms by one bar length
    ax.set_ylabel(x_label)
    ax.set_ylabel(y_label)
    ax.legend(fontsize=12)
    x_ticks_positions = []
    offset = 0
    for histogram in data:
        group_width = len(histogram) * bar_width
        point = group_width / 2 + bar_width / 2
        pos = offset + point
        offset += group_width + step
        x_ticks_positions.append(pos)
    ax.set_xticks(x_ticks_positions)
    # Generate short a-axis tick labels
    short_xticks = ["Exp"+str(i) for i in range(len(exp_labels))]
    ax.set_xticklabels(tuple(short_xticks))
    # Move actual x-axis tick labels outside the plot
    labels = ""
    i = 0
    for label in exp_labels:
        labels += "Exp" + str(i) + " - " + label + "\n"
        i += 1
    ax.text(1.05, 0.95, labels, transform=ax.transAxes, fontsize=10,
                                verticalalignment='top')
    # Set font size for x-axis tick labels
    for item in ax.get_xticklabels():
        item.set_fontsize(10)
    if logscale:
        ax.set_yscale('log')
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    bar_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    bar_plot.clf()

# Generates a latency vs throughput plot for a single varying parameter
def latency_vs_throughput(latencies, rates, plot_repo, varying_parameters,
                          plot_file_prefix, plot_type="line", logscale=False):
    assert len(latencies) > 0
    assert len(rates) > 0
    # TODO (john): Support multiple varying parameters
    assert len(varying_parameters) == 1
    if plot_type == "line":
        labels = []
        tag, values = varying_parameters[0]
        labels.append(str(tag) + ": " + str(values))
        x_axis_label = "Latency [ms]"
        y_axis_label = "Throughput [records/s]"
        plot_filename = plot_file_prefix + "-latency-vs-throughput.png"
        generate_line_plot([latencies], [rates], x_axis_label, y_axis_label,
                           labels, plot_repo, plot_filename,
                           plot_type=="cdf", values, logscale=logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")

# Generates boxplots showing the overhead of
# using the Streaming API over batch queues
def api_overhead(latencies, plot_repo, varying_parameters,
                 plot_file_prefix, plot_type="boxplot", logscale=False):
    assert len(latencies) > 0
    labels = [""] * len(latencies)
    try:  # In case there is at least one varying parameter
        tag, values = varying_parameters
        if len(values) > 0:
            for i in range(0, len(labels) - 1, 2):
                value_str = str(values[i // 2])
                labels[i] = "(" + tag + ": " + value_str + ") - Streaming API"
                labels[i+1] = "(" + tag + ": " + value_str + ") - Plain Queue"
    except TypeError: # All parameters are fixed
        for i in range(0, len(labels) - 1, 2):
            labels[i] = "Streaming API"
            labels[i+1] = "Plain Queue"
    if plot_type == "boxplot":
        x_axis_label = ""
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-api-overhead.png"
        data = [data for _, data in latencies]
        generate_box_plot(data, x_axis_label, y_axis_label,
                          labels, plot_repo, plot_filename, logscale)
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-api-overhead-cdf.png"
        x_data = [data for _, data in latencies]
        generate_line_plot(x_data, [], x_axis_label, y_axis_label,
                          labels, plot_repo, plot_filename, True,
                          logscale=logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")

# Generates barcharts showing actor rates
def actor_rates(rates, plot_repo, labels,
                 plot_file_prefix, plot_type="barchart", logscale=False):
    assert len(rates) > 0
    if plot_type == "barchart":
        x_axis_label = ""
        y_axis_label = "Actor processing rate [records/s]"
        plot_filename = plot_file_prefix + "-actor-rates.png"
        # Prepare data for barchart
        data = []
        bar_metadata = []
        i = 0
        label_is_set = False  # Add one plot label to indicate the source bars
        for actor_rates in rates:
            data.append([])
            bar_metadata.append([])
            for actor_id, in_rate, _, out_rate, _ in actor_rates:
                hatch = False
                add_label = False
                color = ""
                actor_name = actor_id.split(",")[1].strip()
                if actor_name == "sink":
                    continue
                elif "source" in actor_name.lower():
                    data[i].append(out_rate)
                    hatch = True
                    color = "rosybrown"
                    if not label_is_set:
                        add_label = True
                        label_is_set = True
                else:
                    # We use 'out_rate' because the current logging for task-
                    # based execution does not log 'in_rate'. In queue-based
                    # execution both 'in_rate' and 'out_rate' are logged
                    data[i].append(out_rate)
                bar_metadata[i].append((actor_name,color,hatch,add_label))
            i += 1
        generate_barchart_plot(data, x_axis_label, y_axis_label,
                               bar_metadata, labels, plot_repo,
                               plot_filename, logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")


def parse_labels(labels_filepath):
    labels = []
    print("Reading labels file: {}".format(labels_filepath))
    try:  # Read labels
        with open(labels_filepath,"r") as lf:
            for label in lf:
                labels.append(label.strip())
    except FileNotFoundError:
        sys.exit("Could not find file '{}'".format(labels_filepath))
    return labels

def parse_filenames(labels_filepath, file_repo):
    labels = []
    print("Reading labels file: {}".format(labels_filepath))
    try:  # Read labels
        with open(labels_filepath,"r") as lf:
            for label in lf:
                labels.append(file_repo+label.strip())
    except FileNotFoundError:
        sys.exit("Could not find file '{}'".format(labels_filepath))
    return labels


if __name__ == "__main__":

    args = parser.parse_args()

    # Plotting arguments
    plot_type = str(args.plot_type)
    cdf = bool(args.cdf)
    logscale = bool(args.logscale)
    plot_repo = str(args.plot_repo)
    if plot_repo[-1] != "/":
        plot_repo += "/"
    plot_file_prefix = str(args.plot_file)
    if plot_type == "throughput":
        plot_file_prefix = throughput_plot_file_prefix

    # Input file arguments
    file_repo = str(args.file_repo)
    if file_repo[-1] != "/":
        file_repo += "/"
    input_filenames = str(args.files)
    labels_filepath = str(args.labels)
    latency_file_prefix = str(args.latency_file)
    throughput_file_prefix = str(args.throughput_file)

    plot_id = str(_generate_uuid())
    plot_file_prefix += plot_id
    labels = parse_labels(labels_filepath)
    input_files = parse_filenames(input_filenames, file_repo)

    if plot_type == "latency":
        plot_type = "cdf" if cdf else "line"
        x_label = "Samples"
        y_label = "End-to-end record latency [s]"
        latencies = collect_latencies(input_files)
        x_data = latencies if cdf else [ [s for s in range(len(data))]
                                            for data in latencies]
        y_data = [] if cdf else latencies
        generate_line_plot(x_data, y_data, x_label, y_label, labels,
                           plot_repo, plot_file_prefix, cdf,
                           logscale=logscale)
    elif plot_type == "throughput":
        if cdf:
            sys.exit("CDF are not supported for throughput plots.")
        plot_type = "barchart"
        x_label = "Samples"
        y_label = "End-to-end record latency [s]"
        # Collect rates from log files
        rates = collect_rates(input_files)

        actor_rates(rates, plot_repo, labels,
                    plot_file_prefix, plot_type, logscale)
