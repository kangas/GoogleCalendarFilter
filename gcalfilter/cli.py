import config
import core
import argparse

valid_commands = ["fetch", "report"]

def main():
    args = parse_argv()
    # conf = config.get()

    cmd = args.cmd
    print "CMD=%s" % args.cmd
    dispatch_cmd(cmd, args)

def parse_argv():
    parser = argparse.ArgumentParser(
                 prog="gcalfilter",
                 description="Google Calendar scraper + query thing"
             )
    parser.add_argument("cmd", choices=valid_commands)
    return parser.parse_args()

def dispatch_cmd(cmd, args):
    db = core.getdb()
    core.init_caches(db)
    if cmd == "fetch":
        core.refetch_calendars(db)
    elif cmd == "report":
        report = core.run_report_plaintext(db, "kernel", "Vacations")
        print report
    else:
        raise NotImplementedError("Unsupported cmd: " % cmd)
 
if __name__ == "__main__":
    main()
