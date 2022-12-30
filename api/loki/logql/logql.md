# logql

```
package "github.com/commentlens/loghouse/api/loki/logql"

Query : LogQuery | MetricQuery;

var_name : < upcase | lowcase | number | '_' >;
string : ( '"' { not "\"" } '"' | '`' { not "`" } '`' );
duration : < number > lowcase;

LogQuery : LogSelector PipelinesMaybe;
LogSelector : "{" LogSelectorMembersMaybe "}";
LogSelectorMembersMaybe : empty | LogSelectorMembers;
LogSelectorMembers : LogSelectorMember | LogSelectorMember "," LogSelectorMembers;
LogSelectorMember : LabelKey LabelOp string;
LabelKey : var_name;
LabelOp : "=" | "!=" | "=~" | "!~";

PipelinesMaybe : empty | Pipelines;
Pipelines : Pipeline | Pipeline Pipelines;
Pipeline : LineFilter | LabelFilter;
LineFilter : LineFilterOp string;
LineFilterOp : "|=" | "!=" | "|~" | "!~";
LabelFilter : "|" NestedLabelKey LabelOp string;
NestedLabelKey : var_name | var_name "." NestedLabelKey;

MetricQuery : "sum" "by" "(" "level" ")" "(" "count_over_time" "(" LogQuery "[" duration "]" ")" ")";
```
