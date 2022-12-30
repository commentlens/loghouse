# logql

```
package "github.com/commentlens/loghouse/api/loki/logql"

Query : LogQuery | MetricQuery;

var_name : < lowcase | number | '_' > ;
string : '"' { not "\"" } '"' ;
duration : < number > lowcase ;

LogQuery : LogSelector ;
LogSelector : "{" LogSelectorMembers "}" ;
LogSelectorMembers : LogSelectorMember | LogSelectorMember "," LogSelectorMembers ;
LogSelectorMember : LabelKey LabelOp LabelValue ;
LabelOp : "=" | "!=" | "=~" | "!~" ;
LabelKey : var_name ;
LabelValue : string ;

MetricQuery : "sum" "by" "(" "level" ")" "(" "count_over_time" "(" LogQuery "[" duration "]" ")" ")" ;
```
