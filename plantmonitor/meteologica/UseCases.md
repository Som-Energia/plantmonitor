# Use Cases

## Use Case 1

### Description

The user wants to insert the forecast data records from meteologica into the database, which does not have any records for said period.

When the user requests data from Meteologica that has not been inserted into the plantmonitor forecast database, the script follows these steps.

### Precondition

Empty tables. The forecast tables don't hold any data from the requested period.

### Sequence

1. User request input
 * From date + to date
 * From date, 30 days from requested _From date_
 * If not specified, 30 days from current date
2. Send request to Meteologica
3. Create new forecast description entry and insert all the forecast data

## Use Case 2

### Description

The user wants to update plantmonitor's forecast records with newly predicted ones.

The script replaces old DB data with the new ones imported from Meteologica API.

### Precondition

The script has uploaded Meteologica's data at some point in the past.

### Sequence

1. The script creates a date range using user's data range or 30 days from today.
2. The script downloads the new data from Meteologica API of given range.
3. The script makes a query to plantmonitor DB with the data range.
4. The script deletes the data selected with the previous search.
5. The script inserts the new data from Meteologica API.

## Use case 3

### Description

The user wants to keep old records without updating past predictions.

The script only fills the missing data records leaving the old predictions untouched.

### Precondition

Data has been uploaded previously but the database has missing records over small periods of time for whatever reason.

### Sequence

1. The script queries the plantmonitor database with a date range.
2. The script inserts the new data from Meteologica into the missing records of the plantmonitor's database, keeping the old, available records.
