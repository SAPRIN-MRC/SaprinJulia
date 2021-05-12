using SAPRINCore
using Dates

@info "Started execution $(now())"
t = now()
#yrage_episodes("DIMAMO")
yrage_episodeQA("DIMAMO")
@info "Finished DIMAMO $(now())"
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
