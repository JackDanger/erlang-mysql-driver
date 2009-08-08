require 'rake/clean'

if (vertest = `erl -noshell -eval 'io:format("~n~s~n", [erlang:system_info(otp_release)]).' -s erlang halt | tail -n 1`.chomp) =~ /(R\d\d[AB])/
	OTPVERSION = $1
else
	STDERR.puts "unable to determine OTP version! (I got #{vertest})" and exit(-1)
end
ERLC_FLAGS = "-Iinclude -D #{OTPVERSION} +warn_unused_vars +warn_unused_import +warn_exported_vars +warn_untyped_record"


CLEAN.include("ebin/*.beam")
directory 'ebin'
rule ".beam" => ["%{ebin,src}X.erl"] do |t|
	sh "erlc -pa ebin -W #{ERLC_FLAGS} +warn_missing_spec -o ebin #{t.source} "
end

CLEAN.include("debug_ebin/*.beam")
directory 'debug_ebin'
rule ".beam" => ["%{debug_ebin,src}X.erl"]  do |t|
	sh "erlc +debug_info -D EUNIT -pa debug_ebin -W #{ERLC_FLAGS} -o debug_ebin #{t.source} "
end

SRC = FileList['src/*.erl']

task :default => ['ebin'] +       SRC.pathmap("%{src,ebin}X.beam")
task :debug   => ['debug_ebin'] + SRC.pathmap("%{src,debug_ebin}X.beam")
task :test    => :debug do
  sh "erlc test/mysql_test.erl && erl -eval \"mysql_test:test().\" -s erlang halt"
end