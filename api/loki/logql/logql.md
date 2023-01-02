# logql

```
package "github.com/commentlens/loghouse/api/loki/logql"

Query : LogQuery | MetricQuery;

var_name : ( upcase | lowcase | '_' ) { upcase | lowcase | '_' | number };
string : ( '"' { not "\"" } '"' | '`' { not "`" } '`' );
duration : '[' { not "]" } ']';

LogQuery : LogSelector PipelinesMaybe;
LogSelector : "{" LogSelectorMembersMaybe "}";
LogSelectorMembersMaybe : empty | LogSelectorMembers;
LogSelectorMembers : LogSelectorMember | LogSelectorMember "," LogSelectorMembers;
LogSelectorMember : LabelKey LogSelectorOp string;
LabelKey : var_name;
LogSelectorOp : "=" | "!=" | "=~" | "!~";

PipelinesMaybe : empty | Pipelines;
Pipelines : Pipeline | Pipeline Pipelines;
Pipeline : LineFilter | DataFilter;
LineFilter : LineFilterOp string;
LineFilterOp : "|=" | "!=" | "|~" | "!~";
DataFilter : "|" string DataFilterOp string;
DataFilterOp : "=" | "!=" | "=~" | "!~" | ">=" | ">" | "<=" | "<";

MetricQuery : "sum" "by" "(" "level" ")" "(" "count_over_time" "(" LogQuery duration ")" ")";
```
