using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames
using JSON
using SAPRINCore

s = SAPRINCore.Settings()

individuals = DataFrame()
SAPRINCore.readindividuals!(s,individuals)
nrow(individuals)