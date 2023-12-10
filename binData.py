import pandas as pd
import sys

def main(data):
    bins = pd.qcut(data, 6)
    print(bins)
    # First bin range defines averages associated with `darkBlue` color, last bin range is `darkRed` color, etc...

if __name__ == '__main__':
    main(sys.argv[1])
