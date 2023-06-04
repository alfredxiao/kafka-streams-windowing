
#
kafka-console-producer --bootstrap-server localhost:9092 --topic source --property "parse.key=true" --property "key.separator=|"

: '
alfred|2000-01-01T02:00:00Z
alfred|2000-01-01T02:02:00Z
alfred|2000-01-01T02:11:00Z
brian|2000-01-01T03:00:00Z
brian|2000-01-01T03:02:00Z
brian|2000-01-01T03:11:00Z
cameron|2000-01-01T04:00:00Z
cameron|2000-01-01T04:02:00Z
cameron|2000-01-01T04:11:00Z
'

# or can do it in oneline
echo -e "derek|2000-01-01T05:00:00Z\nderek|2000-01-01T05:11:00Z" | kafka-console-producer --bootstrap-server localhost:9092 --topic source --property "parse.key=true" --property "key.separator=|"
