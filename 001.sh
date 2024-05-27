#!/bin/bash

echo $@
exit_code=0
if [ $DEBUG_MODE = "true" ]; then
  set -x
fi

# job arguments

PROVIDER=$1
PROVIDER_MATCH=$2
INSTRUMENTS_MATCH=$3
RUN_MODE_=$4
LOCK_PROVIDER=$5
LOCK_INSTRUMENT=$6
LOCK_DELTA=$7
STATUS=$8

echo "current unix user: ${USER}"

if [[ $LOCK_INSTRUMENT == "null" ]]; then
  echo "################ LOCK snapshot not needed for $PROVIDER process"
  exit 0
fi

# kinit arguments
KERBEROS_KEYTAB_PATH=@spark.history.kerberos.keytab@
KERBEROS_USER_PRINCIPAL=@spark.history.kerberos.principal@

hdfs dfs -get @APP_STORAGE_URL@/glx/keytab/${USER}/${USER}.keytab

IFS='#'
arrProvider=($PROVIDER)
unset IFS
IFS=','
arrProvidersMatch=($PROVIDERS_MATCH)
arrInstrumentsMatch=($INSTRUMENTS_MATCH)
unset IFS

#OPTION/FUTURE snapshots paths
OPT_SNAP_PATH=@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prx_format/option/option_snapshot_flag
FUT_SNAP_PATH=@LAKE_STORAGE_URL@/glx/referential/listed_derivates_cross_ref/prx_format/future/future_snapshot_flag

#LOCK / UNLOCK in fedding module
for i in "${arrProvider[@]}"; do
  if [[ $i =~ $LOCK_INSTRUMENT ]]; then

    if [[ $STATUS == "LOCK" ]]; then

      echo "############### Starting LOCK [ $i } snapshot "

      if [[ $i == "LD_OPTION" ]] || [[ $i == "ALL" ]]; then
        echo "LOCK $OPTION_DATE option snapshot : delete _UNLOCK_STATIC and create _LOCK_static and _UNLOCK_STATIC"

        hadoop fs -rm -f $OPT_SNAP_PATH/_UNLOCK_STATIC
        hadoop fs -touchz $OPT_SNAP_PATH/_LOCK_STATIC
        hadoop fs -rm -f $OPT_SNAP_PATH/_DELTA_START_STATUS
        hadoop fs -rm -f $OPT_SNAP_PATH/_DELTA_END_STATUS
        hadoop fs -touchz $OPT_SNAP_PATH/_DELTA_START_STATUS
        hadoop fs -touchz $OPT_SNAP_PATH/_DELTA_END_STATUS
        if [ "$?" -ne "0" ]; then
          exit -1
        fi
      fi
      if [[ $i == "LD_FUTURE" ]] || [[ $I == "ALL" ]]; then
        echo "LOCK $FUTURE_DATE future snapshot : delete _UNLOCK_STATIC and create _LOCK_STATIC"

        hadoop fs -rm -f $FUT_SNAP_PATH/_UNLOCK_STATIC
        hadoop fs -touchz $FUT_SNAP_PATH/_LOCK_STATIC
        if [ "$?" -ne "0" ]; then
          exit -1
        fi
      fi

    elif [[ $STATUS == "UNLOCK" ]]; then

      echo "###################### Starting UNLOCK [ $i ] snapshot "

      if [[ $ == "LD_OPTION" ]] || [[ $i == "ALL" ]]; then

        echo "UNLOCK $OPTION_DATE option snapshot  :  create _UNLOCK_STATIC and delete _LOCK_STATIC"

        hadoop fs -rm -f $OPT_SNAP_PATH
        hadoop fs - touchz $_FUT_SNAP_PATH/_LOCK_STATIC
        if [ "$?" -n "0" ]; then
          exit -1
        fi
      fi
    fi
  fi
done

#LOCK / UNLOCK in updating module and delta processing

for i in "${arrProvidersMatch[@]}"; do

  if [[ $LOCK_PROVIDER == *"|$i|"* ]] || [[ $LOCK_DELTA == *"|$i|"* ]]; then
    if [[ $STATUS == "LOCK" ]]; then

      echo "###############################Starting LOCK [ $i ] snapshot "

      if [[ $INSTRUMENTS_MATCH =~ "OPTION" ]]; then
        echo "LOCK $OPTION_DATE option snapshot : check if it not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC"

        if [[ $LOCK_PROVIDER == *"|$i|"* ]]; then
          hadoop ts -rm -f $OPT_SNAP_PATH/_UNLOCK_STATIC
          hadoop fs -touchz $OPT_SNAP_PATH/_LOCK_STATIC
          if [ "$?" -ne "0" ]; then
            exit -1
          fi
        elif [[ $LOCK_DELTA = *"|$i|"* ]]; then
          hadoop fs -rm -f $OPT_SNAP_PATH/_UNLOCK_DELTA
          hadoop fs -touchz $OPT_SNAP_PATH/_LOCK_DELTA
          echo "$i" | hadoop fs -appendToFile - $OPT_SNAP_PATH/_DELTA_START_STATUS
          if [ "$?" -ne "0" ]; then
            exit -1
          fi
        fi
      fi
      if [[ $INSTRUMENTS_MATCH =~ "FUTURE" ]] && [[ $LOCK_PROVIDER == *"|$i|"* ]]; then
        echo "LOCK $FUTURE_DATE future snapshot : check if is not LOCK yet and delete _UNLOCK_STATIC and create _LOCK_STATIC"

        hadoop fs -rm -f $FUT_SNAP_PATH/_UNLOCK_STATIC
        hadoop fs -touchz $FUT_SNAP_PATH/_LOCK_STATIC
        if [ "$?" -ne "0" ]; then
          exit -1
        fi
      fi

    elif [[ $STATUS == "UNLOCK" ]]; then

      echo "######################### Starting UNLOCK [ $i ] snapshot "

      if [[ $INSTRUMENTS_MATCH =~ "OPTION" ]]; then
        echo "UNLOCK $OPTION_DATE option snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC"

        if [[ $LOCK_PROVIDER == *"|$i|"* ]]; then
          hadoop fs -rm -f $OP_SNAP_PATH/$OPTION_DATE/_LOCK_STATIC
          hadoop fs -touchz $OPT_SNAP_PATH/$OPTION_DATE/_UNLOCK_STATIC
          if [ "$?" -ne "0" ]; then
            exit -1
          fi
        elif [[ $LOCK_DELTA = *"|$i|"* ]]; then
          echo "$i" hadoop fs -appendToFile - $OP_SNAP_PATH/_DELTA_END_STATUS

          deltaStartStatus=$(hadoop fs -cat $OP_SNAP_PATH/_DELTA_START_STATUS | sort)
          deltaEndStatus=$(hadoop fs -cat $OP_SNAP_PATH/_DELTA_END_STATUS | sort)

          if [[ ${deltaStartStatus//[[:blank:]$'\t\r\n']} = ${deltaEndStatus//[[:blank:]$'\t\r\n']} ]]; then
            hadoop fs -rm -f $OPT_SNAP_PATH/_LOCK_DELTA
            hadoop fs -touchz $OPT_SNAP_PATH/_UNLOCK_DELTA
            if [ "$?" -ne "0" ]; then
              exit -1
            fi
          fi
        fi
      fi
      if [[ $INSTRUMENTS_MATCH =~ "FUTURE" ]] && [[ $LOCK_PROVIDER == *"|$i|"* ]]; then
        echo "UNLOCK $FUTURE_DATE future snapshot : create _UNLOCK_STATIC and delete _LOCK_STATIC"

        hadoop fs -rm -f $FUT_SNAP_PATH/_LOCK_STATIC
        hadoop fs -touchz $FUT_SNAP_PATH/_UNLOCK_STATIC
        if [ "$?" -ne "0" ]; then
          exit -1
        fi
      fi
    fi
  fi

done
exti $?
