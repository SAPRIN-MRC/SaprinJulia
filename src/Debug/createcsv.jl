using Arrow
using CSV
using SAPRINCore

arrowtocsv("Agincourt", "Staging", "IndividualMap")
arrowtocsv("Agincourt", "Staging", "LocationMap")
arrowtocsv("Agincourt", "Staging", "HouseholdMap")