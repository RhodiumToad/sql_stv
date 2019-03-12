;;

((sql-mode . ((indent-tabs-mode . t)
	      (tab-width . 4)
	      (eval add-hook 'before-save-hook 'delete-trailing-whitespace nil t))))
