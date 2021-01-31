Samiran@LAPTOP-LT45O27P MINGW64 ~
$ cd

Samiran@LAPTOP-LT45O27P MINGW64 ~
$ cd g:

Samiran@LAPTOP-LT45O27P MINGW64 /g
$ mkdir git-mov

Samiran@LAPTOP-LT45O27P MINGW64 /g
$ cd git-mov

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov
$ git clone https://github.com/banersam/mygithubproject.git
Cloning into 'mygithubproject'...
remote: Enumerating objects: 24, done.
remote: Counting objects: 100% (24/24), done.
remote: Compressing objects: 100% (21/21), done.
remote: Total 24 (delta 5), reused 8 (delta 1), pack-reused 0
Unpacking objects: 100% (24/24), 7.43 KiB | 2.00 KiB/s, done.

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov
$ ll
total 0
drwxr-xr-x 1 Samiran 197609 0 Jan 31 19:29 mygithubproject/

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov
$ cd my*

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git remote rm origin

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git filter-branch --subdirectory-filter mygithubproject -- --all
WARNING: git-filter-branch has a glut of gotchas generating mangled history
         rewrites.  Hit Ctrl-C before proceeding to abort, then use an
         alternative filtering tool such as 'git filter-repo'
         (https://github.com/newren/git-filter-repo/) instead.  See the
         filter-branch manual page for more details; to squelch this warning,
         set FILTER_BRANCH_SQUELCH_WARNING=1.
Proceeding with filter-branch...

Found nothing to rewrite

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ mkdir new-folder

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ mv * new-folder
mv: cannot move 'new-folder' to a subdirectory of itself, 'new-folder/new-folder'

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git add .

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git commit
hint: Waiting for your editor to close the file...
add
error: There was a problem with the editor '"C:\\Program Files (x86)\\Notepad++\\notepad++.exe" -multiInst -notabbar -nosession -noPlugin'.
Please supply the message using either -m or -F option.


Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ add
bash: add: command not found

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git commit
[master ff559af] added
 5 files changed, 0 insertions(+), 0 deletions(-)
 rename README.md => new-folder/README.md (100%)
 rename aws-log-exporter.py => new-folder/aws-log-exporter.py (100%)
 rename dynamodb.py => new-folder/dynamodb.py (100%)
 rename email-trigger.py => new-folder/email-trigger.py (100%)
 rename welcome.py => new-folder/welcome.py (100%)

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ git status
On branch master
nothing to commit, working tree clean

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ pwd
/g/git-mov/mygithubproject

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ ^C

Samiran@LAPTOP-LT45O27P MINGW64 /g/git-mov/mygithubproject (master)
$ cd g:

Samiran@LAPTOP-LT45O27P MINGW64 /g
$ mkdir repo2

Samiran@LAPTOP-LT45O27P MINGW64 /g
$ cd repor2
bash: cd: repor2: No such file or directory

Samiran@LAPTOP-LT45O27P MINGW64 /g
$ cd repo2

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2
$ ll
total 0

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2
$ git clone https://github.com/banersam/python-programing.git
Cloning into 'python-programing'...
remote: Enumerating objects: 19, done.
remote: Counting objects: 100% (19/19), done.
remote: Compressing objects: 100% (15/15), done.
remote: Total 19 (delta 4), reused 8 (delta 1), pack-reused 0
Unpacking objects: 100% (19/19), 14.63 KiB | 12.00 KiB/s, done.

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2
$ ll
total 0
drwxr-xr-x 1 Samiran 197609 0 Jan 31 19:36 python-programing/

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2
$ cd pyth*

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2/python-programing (main)
$ git remote add repo-A-branch /g/git-mov/mygithubproject

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2/python-programing (main)
$ git pull repo-A-branch master --allow-unrelated-histories
warning: no common commits
remote: Enumerating objects: 26, done.
remote: Counting objects: 100% (26/26), done.
remote: Compressing objects: 100% (23/23), done.
remote: Total 26 (delta 5), reused 0 (delta 0), pack-reused 0
Unpacking objects: 100% (26/26), 7.61 KiB | 5.00 KiB/s, done.
From G:/git-mov/mygithubproject
 * branch            master     -> FETCH_HEAD
 * [new branch]      master     -> repo-A-branch/master
Merge made by the 'recursive' strategy.
 new-folder/README.md           |   2 +
 new-folder/aws-log-exporter.py |  74 ++++++++++++++++++++++++
 new-folder/dynamodb.py         |  75 +++++++++++++++++++++++++
 new-folder/email-trigger.py    | 124 +++++++++++++++++++++++++++++++++++++++++
 new-folder/welcome.py          |   3 +
 5 files changed, 278 insertions(+)
 create mode 100644 new-folder/README.md
 create mode 100644 new-folder/aws-log-exporter.py
 create mode 100644 new-folder/dynamodb.py
 create mode 100644 new-folder/email-trigger.py
 create mode 100644 new-folder/welcome.py

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2/python-programing (main)
$ git remote rm repo-A-branch

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2/python-programing (main)
$ git push
Enumerating objects: 29, done.
Counting objects: 100% (29/29), done.
Delta compression using up to 4 threads
Compressing objects: 100% (25/25), done.
Writing objects: 100% (28/28), 7.92 KiB | 450.00 KiB/s, done.
Total 28 (delta 6), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (6/6), completed with 1 local object.
To https://github.com/banersam/python-programing.git
   6bea379..26e387a  main -> main

Samiran@LAPTOP-LT45O27P MINGW64 /g/repo2/python-programing (main)
$
