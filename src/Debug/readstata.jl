using DataFrames
using StatFiles

df = StatFiles.load("D:\\Data\\SAPRIN_Data\\DIMAMO\\Episodes\\SurveillanceEpisodesBasicAnon.dta") |> DataFrame