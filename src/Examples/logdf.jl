using SAPRINCore
using DataFrames
using Logging
using Dates
using PrettyTables

l = open("logdf.log", "a+")
io = IOContext(l, :displaysize => (100, 100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
flush(io)

df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);
f = frequency(df, :b)
pretty_table(io, f; alignment=[:c, :r], show_subheader=false)

@info "CrossTab DF\n" df

flush(io)
global_logger(old_logger)
close(io)
