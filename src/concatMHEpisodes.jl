using Dates
using Arrow
using DataFrames
using SAPRINCore

agincourt = Arrow.Table(joinpath(episodepath("Agincourt"), "IndividualExposureEpisodes.arrow")) |> DataFrame
allepisodes = copy(agincourt)
dimamo = Arrow.Table(joinpath(episodepath("DIMAMO"), "IndividualExposureEpisodes.arrow")) |> DataFrame
ahri = Arrow.Table(joinpath(episodepath("AHRI"), "IndividualExposureEpisodes.arrow")) |> DataFrame
append!(allepisodes,dimamo)
append!(allepisodes,ahri)
open(joinpath("D:\\Data\\SAPRIN_Data", "IndividualExposureEpisodesAll.arrow"), "w") do io
    Arrow.write(io, allepisodes, compress=:zstd)
end
#h = combine(groupby(allepisodes,:IsUrbanOrRural), nrow => :n)
#
arrowtostatar("D:\\Data\\SAPRIN_Data", "IndividualExposureEpisodesAll", "IndividualExposureEpisodesAll")
runstata("label_individualexposureepisodes.do", settings.Version, "All nodes", joinpath("D:\\Data\\SAPRIN_Data","IndividualExposureEpisodesAll.dta"))
#