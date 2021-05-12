using Arrow
using CSV
using SAPRINCore

arrowtocsv("DIMAMO", "Staging", "IndividualMap")
arrowtocsv("DIMAMO", "Staging", "LocationMap")
arrowtocsv("DIMAMO", "Staging", "HouseholdMap")