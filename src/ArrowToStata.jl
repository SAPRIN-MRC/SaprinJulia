using Dates
using Arrow
using DataFrames
using TableBrowse
using RCall

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "AHRI", "Episodes", "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
Arrow.write(joinpath("D:\\Data\\SAPRIN_Data", "AHRI", "Episodes", "SurveillanceEpisodesBasic.arrow"), df, compress = :zstd)
R"""
library(arrow)
library(rio)
x <- read_feather("D:\\Data\\SAPRIN_Data\\AHRI\\Episodes\\SurveillanceEpisodesBasic.arrow")
export(x,"D:\\Data\\SAPRIN_Data\\AHRI\\Episodes\\SurveillanceEpisodesBasic.dta")
"""