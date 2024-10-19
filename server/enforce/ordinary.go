package enforce

func OrdinaryPolicies() []Policy {
	var OrdinaryPolicies = []Policy{
		{"/ck/table/*", POST},
		{"/ck/dist_logic_table/*", POST},
		{"/ck/dist_logic_table/*", DELETE},
		{"/ck/table/*", PUT},
		{"/ck/truncate_table/*", DELETE},
		{"/ck/table/ttl/*", PUT},
		{"/ck/table/readonly/*", PUT},
		{"/ck/table/view/*", PUT},
		{"/ck/table/orderby/*", PUT},
		{"/ck/table/group_uniq_array/*", POST},
		{"/ck/table/group_uniq_array/*", DELETE},
		{"/ck/table/*", DELETE},
		{"/ck/table_all/*", DELETE},
		{"/ck/query_history/*", DELETE},
		{"/ck/open_sessions/*", PUT},
		{"/ck/purge_tables/*", POST},
		{"/ck/archive/*", POST},
		{"/ck/table/dml/*", POST},
	}
	return append(OrdinaryPolicies, GuestPolicies()...)
}
