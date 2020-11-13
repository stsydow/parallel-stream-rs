//https://ptrace.fefe.de/wp/wp.c

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

size_t hash(const char* word) {
  size_t x;
  for (x=0; *word; ++word)
    x = (x + (x << 5)) ^ *word;
  return x;
}

#define HASHTABSIZE 65536

struct node {
  char* word;
  unsigned long count;
  struct node* next;
}* hashtab[HASHTABSIZE];

void* allocassert(void* x) {
  if (!x) {
    fprintf(stderr,"out of memory!\n");
    exit(1);
  }
  return x;
}

struct node** lookup(char* word) {
  struct node** x;
  for (x=&hashtab[hash(word)%HASHTABSIZE]; *x; x=&(*x)->next)
    if (!strcmp(word,(*x)->word))
      break;
  return x;
}

char printfbuf[4096];

int comp(const void* a,const void* b) {
  return (*(struct node**)b)->count - (*(struct node**)a)->count;
}

int main(int argc, char *argv[]) {
  FILE *fp;
  struct node** stab;
  unsigned long i,j,count=0;
  char line[8192];
  if(argc == 2) {

    fp = fopen(argv[1] , "r");
    if(fp == NULL) {
      fprintf(stderr, "Error opening file: %s", argv[1]);
      return(-1);
    }
  } else {
      fprintf(stderr, "usage: %s <text_file>", argv[0]);
      return(-1);
  }

  setvbuf(stdout,printfbuf,_IOFBF,sizeof printfbuf);
  while (fgets(line,sizeof(line), fp)) {
    char* x=line;
    char* y;
    do {
      char z;
      struct node** n;
      while (isspace(*x)) ++x;
      y=x;
      while (*x && !isspace(*x)) ++x;
      if (x==y) continue;
      z=*x;
      *x=0;
      n=lookup(y);
      if (!*n) {
	*n=allocassert(malloc(sizeof(struct node)));
	(*n)->word=allocassert(strdup(y));
	(*n)->count=1;
	(*n)->next=0;
	++count;
      } else
	++(*n)->count;
      *x=z;
    } while (*x);
  }
  fclose(fp);
  stab=allocassert(calloc(count,sizeof(stab[0])));
  for (i=j=0; i<HASHTABSIZE; ++i) {
    struct node* n;
    for (n=hashtab[i]; n; n=n->next) {
      stab[j]=n;
      ++j;
      assert(j<=count);
    }
  }
  qsort(stab,count,sizeof(stab[0]),comp);
  for (i=0; i<count; ++i)
    printf("%ld %s\n",stab[i]->count,stab[i]->word);
  return 0;
}
