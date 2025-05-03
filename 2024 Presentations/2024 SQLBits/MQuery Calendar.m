//----------------- M Pre-flight Checklist -----------------
    //Must start with a "let" statement and end with an "in" statement
    //Every step must end with a comma except the last step
    //When using Table. functions, be sure to reference the previous step name as the table.
    //Not whitespace sensitive, but it is case sensitive.
    //Always name your steps, makes troubleshooting and updating a million times easier!
    //To avoid extra steps, assign data types when creating a column.
    //Follow the DRY philosophy - Don't Repeat Yourself. Use variables and already created columns to avoid having to redo logic
    //If you want to remove columns, do so within power query since it will automatically fix the references in future steps

let
//Use variables in M to set the start date
    StartDate = #date(2021, 12, 1),
    //EndDate is dynamically the start of next year. You can hard code this like StartDate, but that's not recommended. 
    EndDate = #date(Date.Year(DateTime.LocalNow())+1,1,1),
    Length = Duration.Days(EndDate - StartDate),
    Today = DateTime.LocalNow(),

//Create list of dates using our variables. #duration increments by (days,hours,minutes,seconds)
    Source = List.Dates(StartDate, Length, #duration(1, 0, 0, 0)),

//Convert this list to a table in order to add additional columns
    #"Converted to Table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Renamed Date Column" = Table.RenameColumns(#"Converted to Table",{{"Column1", "Date"}}),
    #"Changed Type to Date" = Table.TransformColumnTypes(#"Renamed Date Column",{{"Date", type date}}),

//Create a datekey for easy joins to fact tables
    #"Inserted DateKey" = Table.AddColumn(#"Changed Type to Date", "DateKey", each DateTime.Date([Date]), type date),
    #"Changed DateKey to Int" = Table.TransformColumnTypes(#"Inserted DateKey",{{"DateKey", Int64.Type}}),

//Create columns for year & month
    #"Inserted Year" = Table.AddColumn(#"Changed DateKey to Int", "Year", each Date.Year([Date]), Int64.Type),
    #"Inserted Month" = Table.AddColumn(#"Inserted Year", "Month", each Date.Month([Date]), Int64.Type),
    #"Inserted Month Name" = Table.AddColumn(#"Inserted Month","Month Name", each Date.MonthName([Date]), type text),
    //MMM is very useful for visuals since it eliminates a lot of characters and saves space
    #"Inserted MMM" = Table.AddColumn(#"Inserted Month Name", "MMM", each Text.Start([Month Name], 3), type text),
    #"Added Month Year" = Table.AddColumn(#"Inserted MMM", "Month Year", each Text.Combine({[MMM], Text.From([Year], "en-US")}, " "), type text),

//Create columns for day & day of week
    #"Inserted Day" = Table.AddColumn(#"Added Month Year", "Day", each Date.Day([Date]), Int64.Type),
    #"Inserted Day Name" = Table.AddColumn(#"Inserted Day", "Day Name", each Date.DayOfWeekName([Date]), type text),
    //Sets Sunday as 1 
    #"Inserted Day of Week" = Table.AddColumn(#"Inserted Day Name", "Day of Week", each Date.DayOfWeek(([Date]), Day.Sunday)+1, Int64.Type),
    #"Inserted DDD" = Table.AddColumn(#"Inserted Day of Week", "DDD", each Text.Start([Day Name], 3), type text),

//Create MonthDay to use in visuals
    #"Inserted MonthDay" = Table.AddColumn(#"Inserted DDD", "Month Day", each Text.Combine({[MMM], 
            if Text.Length(Text.From([Day])) = 1 then Text.Combine({"0",Text.From([Day])},"") else Text.From([Day])}, ", "), type text),
    //Need MonthDaySort to avoid visuals sorting alphabetically    
    #"Inserted MonthDaySort" = Table.AddColumn(#"Inserted MonthDay", "Month Day Sort", each Text.Combine({
            if Text.Length(Text.From([Month]))=1 then Text.Combine({"0",Text.From([Month])},"") else Text.From([Month]),
            if Text.Length(Text.From([Day])) = 1 then Text.Combine({"0",Text.From([Day])},"") else Text.From([Day])
            }, ""),type text),

//Create Week columns
    #"Added Weekend" = Table.AddColumn(#"Inserted MonthDaySort", "IsWeekend", each if [Day of Week] = 7 then 1 else if [Day of Week] = 1 then 1 else 0,Int64.Type),
    #"Inserted End of Week" = Table.AddColumn(#"Added Weekend", "End of Week", each Date.EndOfWeek([Date]), type date),
    //EndOfWeek + 1 gets the date for the Sunday after the end of the week. This can be useful if your weeks go Mon - Sun.
    #"Added EndOfWeek+1" = Table.AddColumn(#"Inserted End of Week", "Custom", each Date.AddDays([End of Week],1)),
    //EndOfWeekSunday checks that the current day is not a Sunday before using the end of week +1 column. 
    #"Added EndofWeekSunday" = Table.AddColumn(#"Added EndOfWeek+1", "End of Week Sunday", each if [Day Name] = "Sunday" then [Date] else [Custom],type date),
    #"Removed EndOfWeek+1" = Table.RemoveColumns(#"Added EndofWeekSunday",{"Custom"}),
    //WEDate is used for visuals, it can be helpful for end users to see "WE: " before the date so they know this is rolled up to the week level
    #"Added WEDate" = Table.AddColumn(#"Removed EndOfWeek+1", "W.E. Date", each Text.Combine({"WE : ", Text.From([End of Week Sunday])},"")),
    //Need this for sorting otherwise it will sort alpha-numerically instead of chronologically
    #"Inserted WE Week Number" = Table.AddColumn(#"Added WEDate", "W.E. Week Number", each Date.WeekOfYear([End of Week Sunday])-1, Int64.Type),
    
//Create ___ To Date Columns
    //Check if the date is before the current month or if it is the current month that it's before the current day of the month
    #"Added IsYTD" = Table.AddColumn(#"Inserted WE Week Number", "IsYTD", each 
        if [Month] < Date.Month(Today) then 1 
            else if [Month] = Date.Month(Today) then 
                if [Day] <= Date.Day(Today) then 1 
                    else 0 
                else 0,Int64.Type),
    //Check if day of month is before or equal to today
    #"Added IsMTD" = Table.AddColumn(#"Added IsYTD", "IsMTD", each if [Day] <= Date.Day(Today) then 1 else 0,Int64.Type),
    //Check if day of week is before or equal to today
    #"Added IsWTD" = Table.AddColumn(#"Added IsMTD", "IsWTD", each if [Day of Week] <= (Date.DayOfWeek(Today,Day.Sunday)+1) then 1 else 0,Int64.Type),
    //Check if it's the current year then check if it's ytd
    #"Added IsCurrentYTD" = Table.AddColumn(#"Added IsWTD", "IsCurrentYTD", each 
        if Date.Year(Today)=[Year] then 
            if [IsYTD]=1 then 1 else 0 
        else 0,type number),
    //Check if it's the current month and if it's the current ytd
    #"Added IsCurrentMTD" = Table.AddColumn(#"Added IsCurrentYTD", "IsCurrentMTD", each 
        if Date.Month(Today) = [Month] then
            if [IsMTD] = 1 then 
                if [IsCurrentYTD]=1 then 1 //always reuse code whenever possible! 
                else 0 
            else 0 
        else 0,type number),
    //Check if it's the same week of the year then if it's week to date then if it's within the current year.
    #"Added IsCurrentWTD" = Table.AddColumn(#"Added IsCurrentMTD", "IsCurrentWTD", each 
        if Date.WeekOfYear(Today) = Date.WeekOfYear([Date]) then 
            if [IsWTD] = 1 then 
                if [IsCurrentYTD] =1 then 1 
                else 0 
            else 0 
        else 0,type number),

//Create ___ Ago Columns    
    //if it's within the current year then take the month diff, otherwise do some math to account for how many years ago it is
    #"Added MonthsAgo" = Table.AddColumn(#"Added IsCurrentWTD", "MonthsAgo", each 
        (if [Year] = Date.Year(Today) then Date.Month(Today) - [Month]
            else Date.Month(Today) - [Month] + (12 * (Date.Year(Today) - [Year])))*-1, Int64.Type),
    //use duration days function to get the date difference in days
    #"Added DaysAgo" = Table.AddColumn(#"Added MonthsAgo", "DaysAgo", each 
        Duration.Days(Date.From(Today)-Date.From([Date])),Int64.Type),
    #"Added WeeksAgo" = Table.AddColumn(#"Added DaysAgo", "Weeks Ago (W.E.)", each 
        if [Year] = Date.Year(Today) then Date.WeekOfYear(Today) - [W.E. Week Number]
        else Date.WeekOfYear(Today) - [W.E. Week Number] + 52, type number),

//Create work day columns
    //Work day of the week reassigns Sunday & Saturday to Friday
    #"Added WorkDayOfWeek" = Table.AddColumn(#"Added WeeksAgo", "Work Day of Week", each 
        Date.DayOfWeek(
            (if [Day of Week] =1 then Date.AddDays([Date],-2) else
            if [Day of Week] = 7 then Date.AddDays([Date],-1) else
            [Date])
        ,Day.Sunday) +1,Int64.Type), //+1 accounts for the fact that Day.Sunday sets Sunday as 0.
    //Feel free to review the math here, but it took quite a few napkins to create this one
    #"Added WorkDaysAgo" = Table.AddColumn(#"Added WorkDayOfWeek", "Work Days Ago", each 
        (if ([Work Day of Week] - (Date.DayOfWeek(Today,Day.Sunday)+1 )) > 0 
            then 5 + (Date.DayOfWeek(Today,Day.Sunday)+1 ) - [Work Day of Week] 
            else ([Work Day of Week] - (Date.DayOfWeek(Today,Day.Sunday)+1 )))
        +
        (Number.RoundDown([DaysAgo]/7)*5),Int64.Type),

//Feel free to add more holidays. This is currently limited to holidays that are date determined, not moon cycle determined nor day of the month determined
    #"Added Holiday" = Table.AddColumn(#"Added WorkDaysAgo", "IsHoliday", each if [Month Day] = "Jan 1" then 1 else if [Month Day] = "July 4" then 1 else if [Month Day] = "Nov 11" then 1 else if [Month Day] = "Dec 25" then 1 else 0,Int64.Type),
//Naming date "CalendarDate" makes it much easier to find when building visuals since there are usually quite a few date columns
    #"Renamed Date for Clarity" = Table.RenameColumns(#"Added Holiday",{{"Date", "CalendarDate"}})
in
    #"Renamed Date for Clarity"