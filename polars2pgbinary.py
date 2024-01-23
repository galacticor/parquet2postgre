import argparse

import pyarrow.dataset as ds
from pgpq import ArrowToPostgresBinaryEncoder


def main(path_read: str, path_save: str):
    dataset = ds.dataset(path_read)
    batches = dataset.to_batches(dataset)
    encoder = ArrowToPostgresBinaryEncoder(dataset.schema)
    with open(path_save, mode='wb') as f:
        f.write(encoder.write_header())
        for batch in batches:
            f.write(encoder.write_batch(batch))
        f.write(encoder.finish())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path_read', type=str)
    parser.add_argument('path_save', type=str)
    args = parser.parse_args()
    main(args.path_read, args.path_save)
