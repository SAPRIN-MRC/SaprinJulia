using Dates
using Arrow
using DataFrames
using SAPRINCore

#produce_mhepisodes("AHRI")
# agincourt = Arrow.Table(joinpath(episodepath("Agincourt"), "IndividualExposureEpisodes.arrow")) |> DataFrame
# a = combine(groupby(agincourt,:IsUrbanOrRural), nrow => :n)
# dimamo = Arrow.Table(joinpath(episodepath("DIMAMO"), "IndividualExposureEpisodes.arrow")) |> DataFrame
# d = combine(groupby(dimamo,:IsUrbanOrRural), nrow => :n)
ahri = Arrow.Table(joinpath(episodepath("AHRI"), "IndividualExposureEpisodesTmp.arrow")) |> DataFrame
h = combine(groupby(ahri,:IsUrbanOrRural), nrow => :n)
# t = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "IndividualExposureEpisodesAll.arrow")) |> DataFrame
# h = combine(groupby(t,:IsUrbanOrRural), nrow => :n)
