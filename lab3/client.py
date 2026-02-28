import argparse
import logging
import os
import sys


_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main() -> None:
    from lab1.client import main as lab1_main

    lab1_main()


if __name__ == "__main__":
    main()
