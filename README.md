# Warning
* **Github classroom behaves differently than Github. If you don't read these instructions, you will mess up your repository 💯% guaranteed, no matter how well you know git.** It will take more time to fix the mess than to read the instructions below.

# What is inside?
* This file explains how to get your own private repository from CMPSC/DS 410. You will use it to get your assignments and submit your work.
* A quick guide to the basics of working with git on the command line.

# Step 0: Access Tokens

Github does not allow password authentication from the command line. Instead, you will need to use an access token in place of a password. To learn how to make one, go here: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token . For easiest results, use the _classic_ token and click the "repo" box (make sure to set expiration date after the semester and final are over). More adventurous students can try the (better) fine-grained token. For fine-grained tokens, choose "only select repositories" and select your github classroom repository (see below), then in "Permissions" find "Contents" and select "read and write" access.

# Step 1: Create your google classroom assignment repository:
<<<<<<< HEAD
Sign into github. Then go here https://classroom.github.com/a/TjOrC3yY and use your psu email address to join.

# Step 2: Do this every time you are asked to clone your repository.
*  Open up a terminal, connect to our cluster, and navigate to the folder you want to contain your repository.
*  Type ```git clone https://github.com/psu-cmpscds410/homework-Fall2024-[github-username].git ds410hw```
=======
Sign into github. Then go here https://classroom.github.com/a/MW_nVZfd and use your psu email address to join (or click "skip this step").

# Step 2: Do this every time you are asked to clone your repository.
*  Open up a terminal, connect to our cluster, and navigate to the folder you want to contain your repository.
*  Type ```git clone https://github.com/psu-ds410/hws-fall2024-[github-username].git ds410hw```
>>>>>>> 9864bb8c2d0b0eea540b69967d76e291cfcf3d96
   *   🤦 alert: do not literally type in "[github-username]" <---- means to put in your actual github user name (no square brackets).
   *   Use your **access token**, not your **password** when you are asked to authenticate. If you want to copy/paste, do **not** use ctrl-c. Just right click on your terminal and use the paste option. The token will not appear visibly when you type it or copy/paste it (you will not see stars ******* either).
   *   You will now see a new folder called ds410hw. Congratulation 🎉 you have cloned the repository, but you are not done yet.
   *   Go into the folder (type **cd ds410hw**). Otherwise most git commands will mysteriously not work (ok it is not really a mystery: you are not inside a git repo until you do this step).
   *   Tell git who you are:
       *   **git config --global user.name "Put your _name_ inside these quotes "** (🤦 alert)
       *   by the way, pay attention to the spacing. don't add spaces wherever you feel like it. It doesn't work this way for English (an ice man ≠ a nice man) and it doesn't work this way for computers (--global ≠ - - global).
       *   **git config --global user.email "Put your _email_ inside these quotes"** (🤦 alert)
*  Do you like to type in the 78728372-digit access token?
   *   If not, type **git config --global credential.helper store**
   *   Next time you type (or copy/paste) your token, git will remember it.
* You should see a setup script inside your repository. Run it like this: **source ./setup** (this will not work on Windows, do this on the cluster). This will allow you to receive new assignments. 
  
# Getting new assignments
1.   Make sure all your existing work has been pushed (use git status and the add/commit/push commands as necessary, see the git quickstart guide below).
2.   Go into your ds410hw folder.
3.   Type **source ./updatestarter**
  <!--
*   If you get a weird message about divergent histories, type **git config pull.rebase false** then type **source ./updatestarter** again.
--> 
<<<<<<< HEAD
=======
*   You might see a weird text editor in your terminal. At the bottom it should say something like press ^X to exit. If you have a windows machine, it means you should press the control key and x at the same time (it should be similar on a mac).
>>>>>>> 9864bb8c2d0b0eea540b69967d76e291cfcf3d96
*   If you get a message like this:
```
 On branch main
Your branch is ahead of 'origin/main' by 2 commits.
  (use "git push" to publish your local commits)
```
then go back to step 1.
*   If you get a message about merge conflicts, read about how to fix them below.
4.   After you run updatestarter, you have to commit and push (see below).

# Git basics

Git is a popular distributed version control system. There is a very nice online book that explains how to use it https://git-scm.com/book/en/v2 . A distributed version control system allows you to update your code so you **don't** have to create mycode.py,  mycode_v2.py, mycode_v3.py, etc. to keep track of your code's history. Git manages the history of your code.

If you are using a non-linux system, make sure to install git (_git bash_ on windows) and then use the git shell (_git bash_ on windows) to type in commands.

A typical basic workflow in git is:
* Initialization: use **git clone** to download the repository on your computer the first time.
* **git status** to see which files you have changed (in case you forget). Use git status heavily to remind you what is left to be done.
* In case you forgot, _use git status heavily to remind you what is left to be done_. Tattoo this on your forehead if you must.
* **git pull** before you edit your files. This ensures you get the latest versions of the file from github.com
* **git add** to stage the files you changed
* **git commit** to update your local repository with the staged files
* **git push** to update your remote repository (the one that is stored on github.com). This will ensure your work is saved even if your computer explodes in a freak accident involving coffee, division by 0, and an asteroid impact.

You can clone a repository on many computers and use them to push changes to the remote repository. In order to synchronize all of these local repositories, you can use **git pull** to download the latest changes. If these changes conflict with yours, you will need to **merge** the changes.

## Cloning a repository (git clone)

Cloning a repository is basically the same as downloading it to your computer for the first time.
The command is 

```git clone [source] [destination]```

The instructions at the beginning explained exactly which repository to clone.

<<<<<<< HEAD
**Important:** you can also access your repository on the web: `https://github.com/psu-cmpscds410/homework-Fall2024-[your-github-username]` (🤦 alert)
=======
**Important:** you can also access your repository on the web: `https://github.com/psu-ds410/hws-fall2024-[your-github-username]` (🤦 alert)
>>>>>>> 9864bb8c2d0b0eea540b69967d76e291cfcf3d96
which is useful because it shows the contents of your remote repository and you can check whether it has what you think it has.

## Staging files (git status and git add)

Let's say I edited some files such as hello.py and world.py. These files need to be staged. This can be done by typing

```git add hello.py world.py```

If you don't remember which files you edited, type

```git status``` 

to see the list.

**Important:** only stage the files you really need (i.e., your source code). **Do not** stage auto-generated files like hello.pyc (since these files are automatically generated by python) and do not stage binary files (i.e., only stage text files like source code).

**Important #2** if you stage a file, and then edit it, you need to stage it again. When you later commit your changes, git will only look at what has been staged. If you edit a file, stage the file, then edit it again (and forget to stage again), ony the first set of edits will be committed.

**Important #3** do not git add a directory (do not do ```git add mydirectory/``` and do not do ```git add *```. List out the specific files, like ```git add mydirectory/myfile.py```

## Committing files (git commit)

After files are staged, you can use git commit to update (save to) your local repository.
Simpy type

```git commit -m "commit message"```

Make the commit message informative to describe what your recent file edits did.
**Important** you are not listing the files to commit. That has already been done when you staged the files.

## Pushing updates (git push)

Committing a your changes only updates your file history on your local machine. In order to save it on the github servers, you need to push the changes using

``` git push origin main```

(sometimes just typing ```git push``` is enough)

## Pulling updates (git pull)

If you have clones of your repositories on different computers, some of them will be out of date. Use 

```git pull origin main``` 

to download the latest changes into your local repository. It is a good idea to git pull before git push.

## Merging conflicts (merge)

Sometimes the changes you pull will conflict with what is on your local repository. For example, 
* on computer 1, you edit hello.py, commit it, then **git push**.
* on computer 2, you then edit hello.py in a different way and commit it. These changes could potentially conflict with the changes in computer 1. Git will not allow you to push this changes directly. It will instruct you to first **git pull** so that the changes you pushed from computer 1 are downloaded onto computer 2. You will then need to edit hello.py on computer 2 to merge in those changes. Finally, you will git add, commit, push the merged changes.

Here is an example of what you might see when there is a merge conflict. Open the file that has a conflict (hello.py). You will see something like this:

```
def greet():
    print("hello world")

<<<<<<< HEAD
def new_function_in_local_repository():
    return 1+1==2
=======
def new_function_in_remote_repository():
    pass
>>>>>>> 3a46f859a1a76724972a9e54e3b14b9d7c9dcd59

if __name__ == "__main__":
    greet()
```

The code in between <<<<<<< HEAD and ======= is the code in my local repository that is not in the remote (on github.com). The code in between ======= and >>>>>>> 3a46f859a1a76724972a9e54e3b14b9d7c9dcd59
is the code that is in the remote repository but I did not have locally. 

It is your decision on how to fix this conflict. Here, I might decide that I want to keep both functions, so I will edit the file to look like this:

```
def greet():
    print("hello world")

def new_function_in_local_repository():
    return 1+1==2

def new_function_in_remote_repository():
    pass

if __name__ == "__main__":
    greet()
```

then I will stage and commit:

```
git add hello.py
git commit -m "merged hello.py"
git push
```

## git branches

Branches are a useful part of git, especially when collaborating on a team. A typical workflow is to create a branch, add a new feature to your code, test it, commit the branch, and then merge it onto the main branch. To learn about branching, the linked book at the start of this section is a good place to start.


# Proper/Improper uses of github

- Github is only for files generated by humans and designed to be edited by humans (the reason is that it keeps track of changes among all the versions of the files you commit). There are a few exceptions but you won't meet them in this class.
   - This includes code you write
   - This includes small test files
   - This DOES NOT include stuff you don't recognize. 
   - When you git add files, **add them by name** (not by **directory** and do not use *)
- The github repository has a file called .gitignore. 
   - you can edit it (don't forget to add/push/commit) to tell it which files should never be in the github repository
   - for example, adding the line *.pyc will make git ignore all files that end in .pyc
   - these files will no longer show up when you type ```git status```
   - on linux and Mac shell, files that start with . do not show up unless you type ```ls -al``` in the command prompt
   - Windows also likes to hide files that start with . (you may need to tell it to show hidden files)
