#!/bin/bash
# Last Modified Date: 20140616-02
export broker_prefix="ASP"
export report_type="ASP-D3"

useConfig="/usr/local/decide/cust/clientConfirmation/v0001/Derivatives_convert2pdf.conf"
commonShell="/usr/local/decide/cust/clientConfirmation/v0001/common_convert2pdf.sh"
if [[ ! -f $useConfig ]]; then
  echo "Configuration file $useConfig not found. Exit."
  exit 1
fi
set -a
. $useConfig
set +a

##############Change the Error Email Notification Setting as below#################
#Only EMAIL_MODE=yes can enable the send email function
#EMAIL_MODE=yes
#email list can contain multiple emails separated by space
EMAIL_LIST="john.chen@serisys.com"
SUBJECT="$DECIDESPACE $report_type Pre-Confirmation Error"

$commonShell "$@"

