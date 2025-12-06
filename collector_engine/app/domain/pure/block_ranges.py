from typing import Iterator


def block_ranges(from_block: int, to_block: int, batch_size: int) -> Iterator[tuple[int, int]]:
    """
    Generate (from_, to_) ranges in [from_block, to_block] with a given batch_size.
    """
    current = from_block
    while current <= to_block:
        end = min(current + batch_size - 1, to_block)
        yield current, end
        current = end + 1
