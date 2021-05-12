using DataFrames
using Dates

function age(dob::Date, date::Date)
    return Dates.year(date) - Dates.year(dob) - ((Dates.Month(date) < Dates.Month(dob)) || (Dates.Month(date) == Dates.Month(dob) && Dates.Day(date) < Dates.Day(dob)))
end
df = DataFrame(:id => [1,2,3,4,5], :DoB => [Date(1990,1,2), Date(2020,2,29), Date(2020,2,29), Date(2000,1,1),Date(1999,12,31)],:DayDate=>[Date(2021,5,5),Date(2021,2,28),Date(2021,3,1),Date(2021,5,5),Date(2021,5,5)])
@time d = transform(df, :DayDate => (x -> Dates.year.(x)) => :CalendarYear, [:DoB,:DayDate] => ((x,y) -> age.(x,y)) => :Age)
