import argparse
from nexmark_generator import NexmarkGenerator

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--calls",
        default=1000,
        type=int,
        help="number of calls that generate data"
    )
    parser.add_argument(
        "--files",
        default=3,
        type=int,
        help="number of files that generated"
    )
    args = parser.parse_args()

    bids_files = []
    persons_files = []
    auctions_files = []

    for i in range(args.files):
        bids_files.append("./data/bids_{}.data".format(i))
        persons_files.append("./data/persons_{}.data".format(i))
        auctions_files.append("./data/auctions_{}.data".format(i))

    data_generator = NexmarkGenerator(args.calls, bids_files, persons_files, auctions_files)
    data_generator.generate_stream()

