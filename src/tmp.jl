using DataFrames: make_unique
using Dates: iterate
using SAPRINCore
using Dates
using Arrow
using DataFrames
using ShiftedArrays
using XLSX
using FreqTables

"Construct day level data from education status observations"
function educationdays(node::String)

end

@info "Started execution $(now())"
t = now()
parentresidentepisodes("DIMAMO")
@info "Finished DIMAMO $(now())"
d = now()-t
@info "Stopped DIMAMO execution $(now()) duration $(round(d, Dates.Second))"
