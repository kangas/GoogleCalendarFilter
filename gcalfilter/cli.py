import config
import core
import argparse

valid_commands = ["fetch", "test"]

def parse_argv():
    parser = argparse.ArgumentParser(
                 prog="gcalfilter",
                 description="Google Calendar scraper + query thing"
             )
    parser.add_argument("cmd", choices=valid_commands)
    return parser.parse_args()

def main():
    args = parse_argv()
    # conf = config.get()

    cmd = args.cmd
    print "CMD=%s" % args.cmd
    if cmd == "fetch":
        db = core.getdb()
        core.refetch_calendars(db)
 
if __name__ == "__main__":
    main()
