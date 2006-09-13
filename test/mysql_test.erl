%% file: mysql_test.erl
%% author: Yariv Sadan (yarivvv@gmail.com)
%% for license see COPYING

-module(mysql_test).
-compile(export_all).

test() ->
    compile:file("/usr/local/lib/erlang/lib/mysql/mysql.erl"),
    compile:file("/usr/local/lib/erlang/lib/mysql/mysql_conn.erl"),
    
    %% Start the MySQL dispatcher and create the first connection
    %% to the database. 'p1' is the connection pool identifier.
    mysql:start_link(p1, "localhost", "root", "password", "test"),

    %% Add 2 more connections to the connection pool
    mysql:connect(p1, "localhost", undefined, "root", "password", "test",
		  true),
    mysql:connect(p1, "localhost", undefined, "root", "password", "test",
		  true),
    
    mysql:fetch(p1, <<"DELETE FROM developer">>),

    mysql:fetch(p1, <<"INSERT INTO developer(name, country) VALUES "
		     "('Claes (Klacke) Wikstrom', 'Sweden'),"
		     "('Ulf Wiger', 'USA')">>),

    %% Execute a query (using a binary)
    Result1 = mysql:fetch(p1, <<"SELECT * FROM developer">>),
    io:format("Result1: ~p~n", [Result1]),
    
    %% Register a prepared statement
    mysql:prepare(update_developer_country,
		  <<"UPDATE developer SET country=? where name like ?">>),
    
    %% Execute the prepared statement
    mysql:execute(p1, update_developer_country, [<<"Sweden">>, <<"%Wiger">>]),
    
    Result2 = mysql:fetch(p1, <<"SELECT * FROM developer">>),
    io:format("Result2: ~p~n", [Result2]),
    
    %% Make some statements
    S1 = <<"INSERT INTO developer(name, country) VALUES "
	  "('Joe Armstrong', 'USA')">>,
    
    S2 = <<"DELETE FROM developer WHERE name like 'Claes%'">>,

    %% Create a transaction
    T1 = mysql:new_transaction(p1),
    T2 = mysql:add_query(T1, S1),
    
    %% You can execute prepared statements inside transactions
    T3 = mysql:add_execute(T2, update_developer_country,
			   [<<"Sweden">>, <<"%Armstrong">>]),

    T4 = mysql:add_query(T3, S2),
    mysql:commit(T4),

    Result3 = mysql:fetch(p1, <<"SELECT * FROM developer">>),
    io:format("Result2: ~p~n", [Result3]),
    
    %% Another way of doing a transaction
    S3 = <<"DELETE FROM developer WHERE country='USA'">>,
    mysql:transaction(p1, fun(T) ->
				  mysql:add_queries(T, [S1,S2,S3])
			  end),
    
    Result4 = mysql:fetch(p1, <<"SELECT * FROM developer">>),
    io:format("Result2: ~p~n", [Result4]),

    %% Transactions are automatically rolled back if any of their queries
    %% results in an error.
    mysql:transaction(
      p1, fun(T) ->
		  mysql:add_queries(T, [<<"DELETE FROM developer">>,
					<<"bad bad query">>])
	  end),

    Result5 = mysql:fetch(p1, <<"SELECT * FROM developer">>),
    io:format("Result2: ~p~n", [Result5]),
				    
    ok.
    
    
