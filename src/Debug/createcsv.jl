using Arrow
using CSV
using SAPRINCore

arrowtocsv("AHRI", "Staging", "IndividualMap")
arrowtocsv("AHRI", "Staging", "LocationMap")
arrowtocsv("AHRI", "Staging", "HouseholdMap")
arrowtocsv("DIMAMO", "Staging", "IndividualMap")
arrowtocsv("DIMAMO", "Staging", "LocationMap")
arrowtocsv("DIMAMO", "Staging", "HouseholdMap")