from django.shortcuts import render, redirect, get_object_or_404
from .models import Author, Quote, Tag
from .form import QuoteForm, AuthorForm


def main(request):
    quotes_list = Quote.objects.all()
    context = {'quotes_list': quotes_list}
    return render(request, 'quoteapp/index.html', context)

def author(request, author_id):   
    author = get_object_or_404(Author, pk=author_id)
    context = {"author": author}    
    return render(request, 'quoteapp/author.html', context)

def authoradding(request):
    if request.user.is_authenticated:
        if request.method == 'POST':
            form = AuthorForm(request.POST)
            if form.is_valid():
                new_author = form.save()
                return redirect(to='quoteapp:main')
            else:
                context = {'form': form} 
                return render(request, 'quoteapp/authoradding.html', context)
        return render(request, 'quoteapp/authoradding.html', {'form': AuthorForm()})
    return redirect(to='quoteapp:main')        

def quote(request):
    if request.user.is_authenticated:
        tags = Tag.objects.all()
        if request.method == 'POST':
            form = QuoteForm(request.POST)
            if form.is_valid():
                new_quote = form.save()
                choice_tags = Tag.objects.filter(name__in=request.POST.getlist('tags'))
                for tag in choice_tags.iterator():
                    new_quote.tags.add(tag)
                return redirect(to='quoteapp:main')
            else:
                context = {"tags": tags, 'form': form} 
                return render(request, 'quoteapp/quote.html', context)
        return render(request, 'quoteapp/quote.html', {"tags": tags, 'form': QuoteForm()})
    return redirect(to='quoteapp:main')        
